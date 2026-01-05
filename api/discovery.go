package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	mdns "github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"optimusdb/app"
	"optimusdb/config"
	"optimusdb/logger"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
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

// PrintDiscoveredPeers periodically prints the list of discovered peers
// Writes discovered peer information to contributions
// Periodically writes new peers to the contributions database.
// Each peer discovery event is added as a JSON entry.
// awaitWriteEvent (service.go) - Listens for write events.
// awaitStoreExchange (service.go) - Replicates contributions DB from peers.
// PrintDiscoveredPeers (discovery.go) - Stores discovered peers in contributions.
// getContri (service.go) - Handles CONTRI command to retrieve contributions.
func PrintDiscoveredPeers(optimusdb *app.KnowledgeBaseDB) {
	ctx := context.Background()
	dbContri := optimusdb.Contributions

	ticker := time.NewTicker(100 * time.Second) // Runs every 100 seconds
	defer ticker.Stop()

	for range ticker.C {
		peerList.Lock() // Prevent concurrent modification
		if len(peerList.peers) == 0 {
			logger.DISc("No peers discovered yet")
		} else {
			logger.DISc("Discovered %d peer(s)", len(peerList.peers))
			logger.DISc("Contributions Store: %v", (*dbContri).Address().String())

			for id, info := range peerList.peers {
				// Ensure the peer has valid addresses before proceeding
				if len(info.Addrs) == 0 {
					logger.Warn("Skipping peer %s due to empty address list", id)
					continue
				}

				// Add the event to the contributions store
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

// Converts Multiaddr array to string array
func convertMultiaddrsToString(addrs []multiaddr.Multiaddr) []string {
	var strAddrs []string
	for _, addr := range addrs {
		strAddrs = append(strAddrs, addr.String())
	}
	return strAddrs
}

// HandlePeerFound is triggered when a new peer is discovered via any method
func (n *DiscoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	logger.DISc("Found peer: %v", pi.ID)

	// Add to peer tracker for HTTP API
	TrackPeer(pi)

	// Register into discovered peer DB
	n.db.AddDiscoveredPeer(string(pi.ID))

	// Add peer to peerstore
	n.host.Peerstore().AddAddr(pi.ID, pi.Addrs[0], peerstore.PermanentAddrTTL)

	// Add peer to global list
	peerList.Lock()
	peerList.peers[pi.ID] = pi
	peerList.Unlock()

	// Attempt connection
	err := n.host.Connect(context.Background(), pi)
	if err != nil {
		logger.Warn("Failed to connect to discovered peer %v: %v", pi.ID, err)
	} else {
		logger.DISc("Successfully connected to peer: %v", pi.ID)
	}
}

// extractIP extracts the IP part from a multiaddr.Multiaddr
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

// StartMdnsDiscovery initializes mDNS-based peer discovery
func StartMdnsDiscovery(h host.Host, mdnsServiceName string) *Service {
	// Define the discovery handler
	peerHandler := &DiscoveryNotifee{host: h}

	// Start mDNS service
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

// stopMdnsDiscovery stops the mDNS service
func (s *Service) stopMdnsDiscovery() {
	if s.mdns != nil {
		s.mdns.Close()
		logger.DISc("mDNS discovery stopped")
	}
}

// WaitForExit listens for termination signals and cleans up
func WaitForExit(service *Service) {
	// Listen for OS termination signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan // Block until a signal is received

	logger.Warn("Received termination signal in Discovery. Cleaning up...")
	service.stopMdnsDiscovery()
	os.Exit(0)
}

// listenPubSub listens for new peer announcements
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

// StopDiscovery cleans up discovery services
func (s *Service) StopDiscovery() {
	// mDNS service
	if s.mdns != nil {
		s.mdns.Close()
		logger.DISc("mDNS discovery stopped")
	}

	// PubSub service
	if s.Pubsub != nil && s.Topic != nil {
		s.Topic.Close()
		logger.DISc("PubSub discovery stopped")
	}

	// DHT service
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

// StartDiscovery initializes all enabled discovery mechanisms
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

		// Initialize Kademlia DHT (full routing mode)
		kademliaDHT, err := dht.New(context.Background(), h, dht.Mode(dht.ModeServer))
		if err != nil {
			logger.Error("Failed to initialize DHT: %v", err)
		} else {
			service.dht = kademliaDHT
			logger.DISc("DHT routing discovery initialized")

			// Start advertising our presence
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

			// Start finding peers periodically
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

	// Start PubSub discovery if enabled
	if *config.FlagAutodiscoveryipfsPubSub {
		logger.DISc("Enabling PubSub-based discovery")

		// FIX #3: Added FloodPublish to improve message delivery in small clusters
		// FloodPublish ensures all peers receive messages even if mesh isn't perfect
		ps, err := pubsub.NewGossipSub(context.Background(), h,
			pubsub.WithPeerExchange(true),
			pubsub.WithFloodPublish(true), // FIX #3: Ensure delivery in 3-node cluster
		)

		if err != nil {
			logger.Error("Failed to initialize PubSub: %v", err)
		} else {
			logger.DISc("GossipSub created with default mesh (crash-safe)")

			topic, err := ps.Join("optimusdb")
			if err != nil {
				logger.Error("Failed to join PubSub topic: %v", err)
			} else {
				sub, err := topic.Subscribe()
				if err != nil {
					logger.Error("Failed to subscribe to topic: %v", err)
				} else {
					service.Pubsub = ps
					service.Topic = topic
					service.Sub = sub
					go service.listenPubSub(peerHandler)
					logger.DISc("Subscribed to optimusdb topic")
				}
			}
		}
	}

	logger.DISc("Discovery service initialization complete")
	return service
}
