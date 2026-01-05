package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	mdns "github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"

	"optimusdb/app"
	"optimusdb/config"
	"optimusdb/logger"
)

// Service represents the discovery service
type Service struct {
	host   host.Host
	mdns   mdns.Service
	dht    *dht.IpfsDHT
	Pubsub *pubsub.PubSub       // ← Capitalized (exported)
	Topic  *pubsub.Topic        // ← Capitalized (exported)
	Sub    *pubsub.Subscription // ← Capitalized (exported)
}

var peerList = struct {
	sync.Mutex
	peers map[peer.ID]peer.AddrInfo
}{peers: make(map[peer.ID]peer.AddrInfo)}

// DiscoveryNotifee implements the peer discovery handler for mDNS
type DiscoveryNotifee struct {
	host host.Host
	db   *app.KnowledgeBaseDB
}

// isOwnAddress checks if an address belongs to the host
func isOwnAddress(h host.Host, addr string) bool {
	for _, myAddr := range h.Addrs() {
		if strings.Contains(myAddr.String(), addr) {
			return true
		}
	}
	return false
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// PrintDiscoveredPeers periodically prints the list of discovered peers
func PrintDiscoveredPeers(optimusdb *app.KnowledgeBaseDB) {
	ctx := context.Background()
	dbContri := optimusdb.Contributions

	ticker := time.NewTicker(100 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		peerList.Lock()
		if len(peerList.peers) == 0 {
			logger.DISc("No peers discovered yet")
		} else {
			logger.DISc("Discovered %d peer(s)", len(peerList.peers))
			logger.DISc("Contributions Store: %v", (*dbContri).Address().String())

			for id, info := range peerList.peers {
				if len(info.Addrs) == 0 {
					logger.Warn("Skipping peer %s due to empty address list", id)
					continue
				}

				optimusdb.ContributionsMtx.Lock()
				err := (*dbContri).Load(ctx, -1)
				if err != nil {
					logger.Error("Failed to load contributions DB: %v", err)
					optimusdb.ContributionsMtx.Unlock()
					peerList.Unlock()
					return
				}

				data := app.Contribution{
					app.GetAgentName(),
					optimusdb.Config.PeerID,
					string(id),
					time.Now(),
					app.GetOwnIP(),
					app.GetPublicIPAddress(),
					extractIPs(info.Addrs),
				}

				dataJSON, err := json.Marshal(data)
				if err != nil {
					logger.Error("Failed to marshal peer contribution: %v", err)
					optimusdb.ContributionsMtx.Unlock()
					continue
				}

				_, err = (*dbContri).Add(ctx, dataJSON)
				if err != nil {
					logger.Error("Failed to store peer information: %v", err)
					optimusdb.ContributionsMtx.Unlock()
					continue
				}

				logger.DISc("Peer ID: %s | Addresses: %v", id, info.Addrs)
				optimusdb.ContributionsMtx.Unlock()
			}
		}
		peerList.Unlock()
	}
}

func extractIPs(addrs []multiaddr.Multiaddr) []string {
	var ips []string
	for _, addr := range addrs {
		ip, err := extractIPFrom(addr)
		if err != nil {
			logger.Warn("Failed to extract IP from %s: %v", addr, err)
			continue
		}
		ips = append(ips, ip)
	}
	return ips
}

func extractIPFrom(addr multiaddr.Multiaddr) (string, error) {
	components := strings.Split(addr.String(), "/")
	for i, component := range components {
		if component == "ip4" || component == "ip6" {
			if i+1 < len(components) {
				return components[i+1], nil
			}
		}
	}
	return "", fmt.Errorf("no IP component found in multiaddr: %s", addr.String())
}

func convertMultiaddrsToString(addrs []multiaddr.Multiaddr) []string {
	var strAddrs []string
	for _, addr := range addrs {
		strAddrs = append(strAddrs, addr.String())
	}
	return strAddrs
}

func (n *DiscoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	logger.DISc("Found peer: %v", pi.ID)

	TrackPeer(pi)
	n.db.AddDiscoveredPeer(string(pi.ID))
	n.host.Peerstore().AddAddr(pi.ID, pi.Addrs[0], peerstore.PermanentAddrTTL)

	peerList.Lock()
	peerList.peers[pi.ID] = pi
	peerList.Unlock()

	err := n.host.Connect(context.Background(), pi)
	if err != nil {
		logger.Warn("Failed to connect to discovered peer %v: %v", pi.ID, err)
	} else {
		logger.DISc("Successfully connected to peer: %v", pi.ID)
	}
}

func extractIP(addr multiaddr.Multiaddr) string {
	components := strings.Split(addr.String(), "/")
	for i, component := range components {
		if component == "ip4" || component == "ip6" {
			if i+1 < len(components) {
				return components[i+1]
			}
		}
	}
	return "no IP component found in multiaddr: " + addr.String()
}

func StartMdnsDiscovery(h host.Host, mdnsServiceName string) *Service {
	peerHandler := &DiscoveryNotifee{host: h}
	mdnsService := mdns.NewMdnsService(h, mdnsServiceName, peerHandler)
	if mdnsService == nil {
		logger.Error("Failed to start mDNS: Service is nil")
		return nil
	}

	err := mdnsService.Start()
	if err != nil {
		logger.Error("Failed to start mDNS: %v", err)
		return nil
	}

	logger.DISc("mDNS discovery started successfully with service name: %s", mdnsServiceName)
	return &Service{
		host: h,
		mdns: mdnsService,
	}
}

func (s *Service) stopMdnsDiscovery() {
	if s.mdns != nil {
		s.mdns.Close()
		logger.DISc("mDNS discovery stopped")
	}
}

func WaitForExit(service *Service) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	logger.Warn("Received termination signal in Discovery. Cleaning up...")
	service.stopMdnsDiscovery()
	os.Exit(0)
}

func (s *Service) listenPubSub(handler *DiscoveryNotifee) {
	for {
		msg, err := s.Sub.Next(context.Background())
		if err != nil {
			logger.Warn("PubSub subscription error: %v", err)
			continue
		}

		peerID, err := peer.Decode(string(msg.Data))
		if err != nil {
			logger.Warn("Failed to decode peer ID from PubSub message: %v", err)
			continue
		}

		handler.HandlePeerFound(peer.AddrInfo{ID: peerID})
	}
}

func (s *Service) StopDiscovery() {
	if s.mdns != nil {
		s.mdns.Close()
		logger.DISc("mDNS discovery stopped")
	}

	if s.Pubsub != nil && s.Topic != nil {
		s.Topic.Close()
		logger.DISc("PubSub discovery stopped")
	}

	if s.dht != nil {
		logger.DISc("Stopping DHT discovery")
		err := s.dht.Close()
		if err != nil {
			logger.Error("Failed to stop DHT: %v", err)
		} else {
			logger.DISc("DHT discovery stopped")
		}
	}

	logger.DISc("All discovery services stopped")
}

// ═══════════════════════════════════════════════════════════════════════════
// ✅ FIX: CreateGossipSubWithDynamicParams - REPLACES OLD HARDCODED VERSION
// ═══════════════════════════════════════════════════════════════════════════
func CreateGossipSubWithDynamicParams(ctx context.Context, h host.Host) (*pubsub.PubSub, error) {
	// Get expected cluster size from environment or use default
	expectedClusterSize := 8 // Default for small clusters
	if envSize := os.Getenv("CLUSTER_SIZE"); envSize != "" {
		if size, err := strconv.Atoi(envSize); err == nil && size > 0 {
			expectedClusterSize = size
			logger.DISc("Using CLUSTER_SIZE from environment: %d", expectedClusterSize)
		}
	}

	// Calculate optimal GossipSub parameters based on cluster size
	var D, Dlo, Dhi int

	if expectedClusterSize <= 10 {
		// FULL MESH for small clusters (3-10 nodes)
		D = expectedClusterSize - 1
		Dlo = max(2, D-1)
		Dhi = expectedClusterSize + 2
		logger.DISc("Configuring for SMALL cluster (full mesh): D=%d, Dlo=%d, Dhi=%d", D, Dlo, Dhi)
	} else if expectedClusterSize <= 50 {
		// PARTIAL MESH for medium clusters (11-50 nodes)
		D = int(math.Sqrt(float64(expectedClusterSize))) + 5
		Dlo = D - 2
		Dhi = D + 5
		logger.DISc("Configuring for MEDIUM cluster (partial mesh): D=%d, Dlo=%d, Dhi=%d", D, Dlo, Dhi)
	} else {
		// SPARSE MESH for large clusters (51+ nodes)
		D = int(math.Log10(float64(expectedClusterSize))) * 10
		Dlo = D - 3
		Dhi = D + 10
		logger.DISc("Configuring for LARGE cluster (sparse mesh): D=%d, Dlo=%d, Dhi=%d", D, Dlo, Dhi)
	}

	// Ensure minimum safety bounds
	D = max(3, D)
	Dlo = max(2, Dlo)
	Dhi = max(D+2, Dhi)

	logger.DISc("Final GossipSub parameters: D=%d, Dlo=%d, Dhi=%d (cluster size: %d)",
		D, Dlo, Dhi, expectedClusterSize)

	// ✅ FIXED: Message ID function with correct signature (uses pubsub_pb.Message)
	messageIDFunc := func(pmsg *pubsub_pb.Message) string {
		h := sha256.New()
		h.Write(pmsg.Data)
		h.Write(pmsg.From)
		return hex.EncodeToString(h.Sum(nil))[:20]
	}

	// Configure GossipSub parameters
	gparams := pubsub.DefaultGossipSubParams()
	gparams.D = D
	gparams.Dlo = Dlo
	gparams.Dhi = Dhi
	gparams.Dscore = max(2, D/2)
	gparams.Dout = max(2, D/3)
	gparams.Dlazy = max(3, D/2)
	gparams.HeartbeatInterval = 1 * time.Second
	gparams.HistoryLength = 12
	gparams.HistoryGossip = 6
	gparams.GossipFactor = 0.3
	gparams.OpportunisticGraftTicks = 40
	gparams.OpportunisticGraftPeers = 3
	gparams.PruneBackoff = 15 * time.Second
	gparams.GraftFloodThreshold = 3 * time.Second
	gparams.FanoutTTL = 45 * time.Second

	// Create GossipSub with dynamic parameters
	ps, err := pubsub.NewGossipSub(ctx, h,
		pubsub.WithMessageIdFn(messageIDFunc),
		pubsub.WithSeenMessagesTTL(3*time.Minute),
		pubsub.WithFloodPublish(expectedClusterSize <= 10), // Only for small clusters
		pubsub.WithPeerExchange(true),
		pubsub.WithGossipSubParams(gparams),
		pubsub.WithDirectConnectTicks(5),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create GossipSub: %w", err)
	}

	logger.DISc("✅ GossipSub created successfully with dynamic parameters")
	return ps, nil
}

// ═══════════════════════════════════════════════════════════════════════════
// ✅ UPDATED: StartDiscovery - NOW RETURNS NIL FOR PUBSUB DISCOVERY
// ═══════════════════════════════════════════════════════════════════════════
func StartDiscovery(h host.Host, knowledgeBaseDB *app.KnowledgeBaseDB) *Service {
	logger.DISc("Starting enhanced peer discovery")

	peerHandler := &DiscoveryNotifee{
		host: h,
		db:   knowledgeBaseDB,
	}

	service := &Service{host: h}

	// Start mDNS if enabled
	if *config.FlagAutodiscoveryMDNS {
		logger.DISc("Enabling mDNS discovery with service: optimusdb-mdns")
		mdnsService := mdns.NewMdnsService(h, "optimusdb-mdns", peerHandler)
		if err := mdnsService.Start(); err != nil {
			logger.Error("Failed to start mDNS: %v", err)
		} else {
			service.mdns = mdnsService
			logger.DISc("mDNS discovery initialized successfully")
		}
	}

	// Start DHT if enabled
	if *config.FlagAutodiscoveryDHT {
		logger.DISc("Enabling DHT discovery")

		kademliaDHT, err := dht.New(context.Background(), h, dht.Mode(dht.ModeServer))
		if err != nil {
			logger.Error("Failed to initialize DHT: %v", err)
		} else {
			service.dht = kademliaDHT
			logger.DISc("DHT routing discovery initialized")

			// Start advertising
			go func() {
				for {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					mh, err := multihash.Sum([]byte(string(service.host.ID())), multihash.SHA2_256, -1)
					if err != nil {
						logger.Error("Failed to generate CID for DHT Provide: %v", err)
						cancel()
						time.Sleep(30 * time.Second)
						continue
					}

					key := cid.NewCidV1(cid.Raw, mh)
					err = service.dht.Provide(ctx, key, true)
					cancel()

					if err != nil {
						logger.Warn("DHT advertise failed: %v", err)
					} else {
						logger.DISc("Successfully advertised on DHT")
					}
					time.Sleep(30 * time.Second)
				}
			}()

			// Start finding peers
			go func() {
				for {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					peerInfo, err := service.dht.FindPeer(ctx, peer.ID("optimusdb-dht"))
					cancel()

					if err != nil {
						logger.Warn("DHT FindPeers failed: %v", err)
						time.Sleep(30 * time.Second)
						continue
					}

					if peerInfo.ID != "" {
						logger.DISc("DHT discovered peer: %v", peerInfo.ID)
						peerHandler.HandlePeerFound(peerInfo)
					}
					time.Sleep(30 * time.Second)
				}
			}()
		}
	}

	// ✅ CRITICAL CHANGE: DO NOT create GossipSub here anymore
	// Let main.go create it with unified parameters
	if *config.FlagAutodiscoveryipfsPubSub {
		logger.DISc("PubSub discovery flag enabled")
		logger.DISc("⚠️  GossipSub will be created by main.go with unified parameters")
		logger.DISc("   (Discovery service will NOT create separate GossipSub)")
	}

	logger.DISc("Discovery service initialization complete")
	return service
}
