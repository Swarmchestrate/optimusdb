package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	mdns "github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"

	"optimusdb/app"
	"optimusdb/config"
	"optimusdb/logger"
)

// ═══════════════════════════════════════════════════════════════════════════
// SERVICE STRUCTURE
// ═══════════════════════════════════════════════════════════════════════════

// Service represents the enhanced discovery service with connection healing
type Service struct {
	host   host.Host
	mdns   mdns.Service
	dht    *dht.IpfsDHT
	db     *app.KnowledgeBaseDB
	ctx    context.Context
	cancel context.CancelFunc

	// Peer tracking
	peersMu sync.RWMutex
	peers   map[peer.ID]*PeerInfo

	// Connection management
	connectionAttempts map[peer.ID]int
	lastAttempt        map[peer.ID]time.Time
	attemptsMu         sync.Mutex
}

// PeerInfo stores detailed information about discovered peers
type PeerInfo struct {
	ID              peer.ID
	Addrs           []multiaddr.Multiaddr
	DiscoveredAt    time.Time
	LastSeen        time.Time
	ConnectionState network.Connectedness
	Retries         int
}

// ═══════════════════════════════════════════════════════════════════════════
// PEER TRACKING
// ═══════════════════════════════════════════════════════════════════════════

// TrackPeer adds or updates a peer in the tracking system
func (s *Service) TrackPeer(pi peer.AddrInfo) {
	s.peersMu.Lock()
	defer s.peersMu.Unlock()

	if existing, ok := s.peers[pi.ID]; ok {
		// Update existing peer
		existing.LastSeen = time.Now()
		existing.Addrs = pi.Addrs
		existing.ConnectionState = s.host.Network().Connectedness(pi.ID)
		logger.DISc("[TRACK] Updated peer %s (state: %s)", pi.ID.String()[:12], existing.ConnectionState)
	} else {
		// Add new peer
		s.peers[pi.ID] = &PeerInfo{
			ID:              pi.ID,
			Addrs:           pi.Addrs,
			DiscoveredAt:    time.Now(),
			LastSeen:        time.Now(),
			ConnectionState: network.NotConnected,
			Retries:         0,
		}
		logger.DISc("[TRACK] Discovered new peer %s (%d addrs)", pi.ID.String()[:12], len(pi.Addrs))
	}
}

// GetPeerInfo retrieves information about a specific peer
func (s *Service) GetPeerInfo(peerID peer.ID) (*PeerInfo, bool) {
	s.peersMu.RLock()
	defer s.peersMu.RUnlock()
	info, ok := s.peers[peerID]
	return info, ok
}

// GetAllPeers returns a snapshot of all tracked peers
func (s *Service) GetAllPeers() map[peer.ID]*PeerInfo {
	s.peersMu.RLock()
	defer s.peersMu.RUnlock()

	snapshot := make(map[peer.ID]*PeerInfo, len(s.peers))
	for id, info := range s.peers {
		snapshot[id] = info
	}
	return snapshot
}

// ═══════════════════════════════════════════════════════════════════════════
// CONNECTION MANAGEMENT WITH RETRY LOGIC
// ═══════════════════════════════════════════════════════════════════════════

// attemptConnection tries to connect to a peer with retry logic
func (s *Service) attemptConnection(pi peer.AddrInfo, maxRetries int) {
	// Skip self
	if pi.ID == s.host.ID() {
		return
	}

	// Check if already connected
	if s.host.Network().Connectedness(pi.ID) == network.Connected {
		logger.DISc("[CONNECT] Already connected to %s", pi.ID.String()[:12])
		s.updateConnectionSuccess(pi.ID)
		return
	}

	// Rate limiting: Don't retry too quickly
	s.attemptsMu.Lock()
	if lastAttempt, ok := s.lastAttempt[pi.ID]; ok {
		if time.Since(lastAttempt) < 5*time.Second {
			s.attemptsMu.Unlock()
			logger.DISc("[CONNECT] Rate limiting connection to %s (too soon)", pi.ID.String()[:12])
			return
		}
	}
	s.lastAttempt[pi.ID] = time.Now()
	s.attemptsMu.Unlock()

	// Try connecting with retries
	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Check if already connected (might have connected via different path)
		if s.host.Network().Connectedness(pi.ID) == network.Connected {
			logger.DISc("[CONNECT] Peer %s connected via alternate path", pi.ID.String()[:12])
			s.updateConnectionSuccess(pi.ID)
			return
		}

		// Attempt connection with timeout
		ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
		err := s.host.Connect(ctx, pi)
		cancel()

		if err != nil {
			// Connection failed
			if attempt < maxRetries {
				backoff := time.Duration(attempt*attempt) * time.Second // 1s, 4s, 9s
				logger.Warn("[CONNECT] Failed to connect to %s (attempt %d/%d): %v. Retrying in %v...",
					pi.ID.String()[:12], attempt, maxRetries, err, backoff)

				s.updateConnectionAttempt(pi.ID, false)

				// Wait before retry
				select {
				case <-time.After(backoff):
					continue
				case <-s.ctx.Done():
					return
				}
			} else {
				// Max retries reached
				logger.Error("[CONNECT] Failed to connect to %s after %d attempts: %v",
					pi.ID.String()[:12], maxRetries, err)
				s.updateConnectionFailure(pi.ID)
				return
			}
		} else {
			// Connection succeeded
			logger.DISc("[CONNECT] ✅ Successfully connected to %s (attempt %d)", pi.ID.String()[:12], attempt)
			s.updateConnectionSuccess(pi.ID)
			return
		}
	}
}

// updateConnectionAttempt records a connection attempt
func (s *Service) updateConnectionAttempt(peerID peer.ID, success bool) {
	s.attemptsMu.Lock()
	defer s.attemptsMu.Unlock()

	if !success {
		s.connectionAttempts[peerID]++
	} else {
		s.connectionAttempts[peerID] = 0
	}
}

// updateConnectionSuccess updates tracking after successful connection
func (s *Service) updateConnectionSuccess(peerID peer.ID) {
	s.attemptsMu.Lock()
	s.connectionAttempts[peerID] = 0
	s.attemptsMu.Unlock()

	s.peersMu.Lock()
	if info, ok := s.peers[peerID]; ok {
		info.ConnectionState = network.Connected
		info.Retries = 0
	}
	s.peersMu.Unlock()
}

// updateConnectionFailure updates tracking after failed connection
func (s *Service) updateConnectionFailure(peerID peer.ID) {
	s.peersMu.Lock()
	defer s.peersMu.Unlock()

	if info, ok := s.peers[peerID]; ok {
		info.Retries++
		info.ConnectionState = network.NotConnected
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// CONNECTION HEALING - PERIODIC MESH REPAIR
// ═══════════════════════════════════════════════════════════════════════════

// StartConnectionHealing runs periodic checks to maintain mesh connectivity
func (s *Service) StartConnectionHealing() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	logger.DISc("[HEAL] Connection healing service started (30s interval)")

	for {
		select {
		case <-s.ctx.Done():
			logger.DISc("[HEAL] Connection healing stopped")
			return
		case <-ticker.C:
			s.healConnections()
		}
	}
}

// healConnections checks all discovered peers and reconnects if needed
func (s *Service) healConnections() {
	s.peersMu.RLock()
	peersSnapshot := make(map[peer.ID]*PeerInfo, len(s.peers))
	for id, info := range s.peers {
		peersSnapshot[id] = info
	}
	s.peersMu.RUnlock()

	if len(peersSnapshot) == 0 {
		logger.DISc("[HEAL] No peers to heal")
		return
	}

	connectedCount := 0
	disconnectedCount := 0
	reconnectedCount := 0

	for peerID, info := range peersSnapshot {
		if peerID == s.host.ID() {
			continue
		}

		currentState := s.host.Network().Connectedness(peerID)

		if currentState == network.Connected {
			connectedCount++
		} else {
			disconnectedCount++
			logger.Warn("[HEAL] Peer %s is disconnected (retries: %d), attempting reconnection...",
				peerID.String()[:12], info.Retries)

			// Skip if too many failed attempts
			s.attemptsMu.Lock()
			attempts := s.connectionAttempts[peerID]
			s.attemptsMu.Unlock()

			if attempts > 10 {
				logger.Warn("[HEAL] Peer %s has failed %d times, skipping", peerID.String()[:12], attempts)
				continue
			}

			// Try reconnecting
			ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
			err := s.host.Connect(ctx, peer.AddrInfo{
				ID:    peerID,
				Addrs: info.Addrs,
			})
			cancel()

			if err == nil {
				logger.DISc("[HEAL] ✅ Reconnected to peer %s", peerID.String()[:12])
				reconnectedCount++
				s.updateConnectionSuccess(peerID)
			} else {
				logger.Error("[HEAL] Failed to reconnect to %s: %v", peerID.String()[:12], err)
				s.updateConnectionAttempt(peerID, false)
			}
		}
	}

	logger.DISc("[HEAL] Status: total=%d, connected=%d, disconnected=%d, reconnected=%d",
		len(peersSnapshot), connectedCount, disconnectedCount, reconnectedCount)

	// Update connection states
	s.peersMu.Lock()
	for peerID := range s.peers {
		s.peers[peerID].ConnectionState = s.host.Network().Connectedness(peerID)
	}
	s.peersMu.Unlock()
}

// ═══════════════════════════════════════════════════════════════════════════
// DISCOVERY NOTIFIER - HANDLES DISCOVERED PEERS
// ═══════════════════════════════════════════════════════════════════════════

// DiscoveryNotifee implements the peer discovery handler
type DiscoveryNotifee struct {
	service *Service
}

// HandlePeerFound is called when a new peer is discovered
func (n *DiscoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	// Skip self
	if pi.ID == n.service.host.ID() {
		return
	}

	// Skip if no addresses
	if len(pi.Addrs) == 0 {
		logger.Warn("[DISCOVERY] Peer %s has no addresses, skipping", pi.ID.String()[:12])
		return
	}

	logger.DISc("[DISCOVERY] Found peer %s with %d address(es)", pi.ID.String()[:12], len(pi.Addrs))

	// Track peer
	n.service.TrackPeer(pi)

	// Add to knowledgeBaseDB
	//n.service.db.AddDiscoveredPeer(string(pi.ID))
	n.service.db.AddDiscoveredPeer(pi.ID.String())

	// Add to peerstore
	n.service.host.Peerstore().AddAddrs(pi.ID, pi.Addrs, peerstore.PermanentAddrTTL)

	// Attempt connection in background (non-blocking)
	go n.service.attemptConnection(pi, 3)
}

// ═══════════════════════════════════════════════════════════════════════════
// MDNS DISCOVERY
// ═══════════════════════════════════════════════════════════════════════════

func (s *Service) startMDNS() error {
	notifee := &DiscoveryNotifee{service: s}
	mdnsService := mdns.NewMdnsService(s.host, "optimusdb-mdns", notifee)

	if err := mdnsService.Start(); err != nil {
		return fmt.Errorf("failed to start mDNS: %w", err)
	}

	s.mdns = mdnsService
	logger.DISc("[MDNS] Started successfully with service name: optimusdb-mdns")
	return nil
}

// ═══════════════════════════════════════════════════════════════════════════
// DHT DISCOVERY
// ═══════════════════════════════════════════════════════════════════════════

func (s *Service) startDHT() error {
	kademliaDHT, err := dht.New(s.ctx, s.host, dht.Mode(dht.ModeServer))
	if err != nil {
		return fmt.Errorf("failed to initialize DHT: %w", err)
	}

	s.dht = kademliaDHT
	logger.DISc("[DHT] Initialized successfully")

	// Start DHT advertising
	go s.runDHTAdvertise()

	// Start DHT peer discovery
	go s.runDHTDiscovery()

	return nil
}

// runDHTAdvertise periodically advertises this node on the DHT
func (s *Service) runDHTAdvertise() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	logger.DISc("[DHT] Starting advertisement loop")

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)

			mh, err := multihash.Sum([]byte(s.host.ID()), multihash.SHA2_256, -1)
			if err != nil {
				logger.Error("[DHT] Failed to generate multihash: %v", err)
				cancel()
				continue
			}

			key := cid.NewCidV1(cid.Raw, mh)
			err = s.dht.Provide(ctx, key, true)
			cancel()

			if err != nil {
				logger.Warn("[DHT] Advertisement failed: %v", err)
			} else {
				logger.DISc("[DHT] Successfully advertised on DHT")
			}
		}
	}
}

// runDHTDiscovery periodically searches for peers on the DHT
func (s *Service) runDHTDiscovery() {
	ticker := time.NewTicker(45 * time.Second)
	defer ticker.Stop()

	logger.DISc("[DHT] Starting discovery loop")

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(s.ctx, 15*time.Second)

			// Generate a CID for the optimusdb namespace
			mh, err := multihash.Sum([]byte("optimusdb"), multihash.SHA2_256, -1)
			if err != nil {
				logger.Error("[DHT] Failed to generate multihash: %v", err)
				cancel()
				continue
			}

			key := cid.NewCidV1(cid.Raw, mh)

			// ✅ FIX: FindProvidersAsync only returns a channel, not (channel, error)
			peerChan := s.dht.FindProvidersAsync(ctx, key, 10)

			foundCount := 0
			for peerInfo := range peerChan {
				if peerInfo.ID != s.host.ID() {
					logger.DISc("[DHT] Discovered peer: %s", peerInfo.ID.String()[:12])
					(&DiscoveryNotifee{service: s}).HandlePeerFound(peerInfo)
					foundCount++
				}
			}

			if foundCount > 0 {
				logger.DISc("[DHT] Discovery cycle complete: found %d peer(s)", foundCount)
			}

			cancel()
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// PERIODIC STATUS REPORTING
// ═══════════════════════════════════════════════════════════════════════════

// StartStatusReporter logs periodic discovery status
func (s *Service) StartStatusReporter() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	logger.DISc("[STATUS] Status reporter started (60s interval)")

	for {
		select {
		case <-s.ctx.Done():
			logger.DISc("[STATUS] Status reporter stopped")
			return
		case <-ticker.C:
			s.reportStatus()
		}
	}
}

// reportStatus logs current discovery and connection status
func (s *Service) reportStatus() {
	s.peersMu.RLock()
	defer s.peersMu.RUnlock()

	totalPeers := len(s.peers)
	connectedCount := 0
	disconnectedCount := 0

	for peerID, info := range s.peers {
		if peerID == s.host.ID() {
			continue
		}

		currentState := s.host.Network().Connectedness(peerID)
		if currentState == network.Connected {
			connectedCount++
		} else {
			disconnectedCount++
		}

		logger.DISc("[STATUS] Peer %s: state=%s, retries=%d, discovered=%s",
			peerID.String()[:12],
			currentState,
			info.Retries,
			time.Since(info.DiscoveredAt).Round(time.Second))
	}

	meshCoverage := 0.0
	if totalPeers > 0 {
		meshCoverage = float64(connectedCount) / float64(totalPeers) * 100
	}

	logger.DISc("[STATUS] Discovery Summary: total=%d, connected=%d (%.1f%%), disconnected=%d",
		totalPeers, connectedCount, meshCoverage, disconnectedCount)
}

// ═══════════════════════════════════════════════════════════════════════════
// SERVICE LIFECYCLE
// ═══════════════════════════════════════════════════════════════════════════

// StartDiscovery initializes and starts all discovery mechanisms
func StartDiscovery(h host.Host, knowledgeBaseDB *app.KnowledgeBaseDB) *Service {
	logger.DISc("═══════════════════════════════════════════════════════════")
	logger.DISc("STARTING ENHANCED DISCOVERY SERVICE")
	logger.DISc("═══════════════════════════════════════════════════════════")

	ctx, cancel := context.WithCancel(context.Background())

	service := &Service{
		host:               h,
		db:                 knowledgeBaseDB,
		ctx:                ctx,
		cancel:             cancel,
		peers:              make(map[peer.ID]*PeerInfo),
		connectionAttempts: make(map[peer.ID]int),
		lastAttempt:        make(map[peer.ID]time.Time),
	}

	// Start mDNS if enabled
	if *config.FlagAutodiscoveryMDNS {
		if err := service.startMDNS(); err != nil {
			logger.Error("[MDNS] Failed to start: %v", err)
		}
	}

	// Start DHT if enabled
	if *config.FlagAutodiscoveryDHT {
		if err := service.startDHT(); err != nil {
			logger.Error("[DHT] Failed to start: %v", err)
		}
	}

	// Note: PubSub/GossipSub is created in main.go for unified parameters
	if *config.FlagAutodiscoveryipfsPubSub {
		logger.DISc("[PUBSUB] PubSub discovery enabled")
		logger.DISc("[PUBSUB] GossipSub will be created by main.go with unified parameters")
	}

	// Start background services
	go service.StartConnectionHealing()
	go service.StartStatusReporter()

	logger.DISc("═══════════════════════════════════════════════════════════")
	logger.DISc("DISCOVERY SERVICE STARTED SUCCESSFULLY")
	logger.DISc("  ✅ mDNS: %v", *config.FlagAutodiscoveryMDNS)
	logger.DISc("  ✅ DHT: %v", *config.FlagAutodiscoveryDHT)
	logger.DISc("  ✅ PubSub: %v", *config.FlagAutodiscoveryipfsPubSub)
	logger.DISc("  ✅ Connection Healing: Enabled (30s)")
	logger.DISc("  ✅ Status Reporter: Enabled (60s)")
	logger.DISc("═══════════════════════════════════════════════════════════")

	return service
}

// StopDiscovery gracefully shuts down all discovery services
func (s *Service) StopDiscovery() {
	logger.DISc("[SHUTDOWN] Stopping discovery service...")

	// Cancel context to stop background goroutines
	s.cancel()

	// Stop mDNS
	if s.mdns != nil {
		s.mdns.Close()
		logger.DISc("[SHUTDOWN] mDNS stopped")
	}

	// Stop DHT
	if s.dht != nil {
		if err := s.dht.Close(); err != nil {
			logger.Error("[SHUTDOWN] Failed to stop DHT: %v", err)
		} else {
			logger.DISc("[SHUTDOWN] DHT stopped")
		}
	}

	logger.DISc("[SHUTDOWN] Discovery service stopped successfully")
}

// ═══════════════════════════════════════════════════════════════════════════
// UTILITY FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// extractIPs extracts IP addresses from multiaddrs
func extractIPs(addrs []multiaddr.Multiaddr) []string {
	var ips []string
	for _, addr := range addrs {
		if ip, err := extractIPFrom(addr); err == nil {
			ips = append(ips, ip)
		}
	}
	return ips
}

// extractIPFrom extracts an IP address from a multiaddr
func extractIPFrom(addr multiaddr.Multiaddr) (string, error) {
	components := strings.Split(addr.String(), "/")
	for i, component := range components {
		if (component == "ip4" || component == "ip6") && i+1 < len(components) {
			return components[i+1], nil
		}
	}
	return "", fmt.Errorf("no IP component found in multiaddr: %s", addr.String())
}

// ═══════════════════════════════════════════════════════════════════════════
// LEGACY COMPATIBILITY - PrintDiscoveredPeers
// ═══════════════════════════════════════════════════════════════════════════

// PrintDiscoveredPeers provides legacy compatibility for contribution logging
func PrintDiscoveredPeers(optimusdb *app.KnowledgeBaseDB) {
	ticker := time.NewTicker(120 * time.Second)
	defer ticker.Stop()

	ctx := context.Background()

	for range ticker.C {
		if optimusdb.Contributions == nil {
			continue
		}

		dbContri := *optimusdb.Contributions
		discoveredPeers := optimusdb.GetDiscoveredPeers()

		if len(discoveredPeers) == 0 {
			logger.DISc("[CONTRIB] No peers discovered yet")
			continue
		}

		logger.DISc("[CONTRIB] Processing %d discovered peer(s)", len(discoveredPeers))

		for _, peerIDStr := range discoveredPeers {
			peerID, err := peer.Decode(peerIDStr)
			if err != nil {
				logger.Warn("[CONTRIB] Invalid peer ID: %v", err)
				continue
			}

			peerAddrs := optimusdb.Node.PeerHost.Peerstore().Addrs(peerID)
			if len(peerAddrs) == 0 {
				continue
			}

			data := app.Contribution{
				AgentName:   app.GetAgentName(),
				Path:        optimusdb.Config.PeerID,
				Contributor: peerIDStr,
				CreationTS:  time.Now(),
				LocalIP:     app.GetOwnIP(),
				NodeIP:      app.GetPublicIPAddress(),
				RemoteIPs:   extractIPs(peerAddrs),
			}

			dataJSON, err := json.Marshal(data)
			if err != nil {
				logger.Error("[CONTRIB] Failed to marshal contribution: %v", err)
				continue
			}

			optimusdb.ContributionsMtx.Lock()
			_, err = dbContri.Add(ctx, dataJSON)
			optimusdb.ContributionsMtx.Unlock()

			if err != nil {
				logger.Error("[CONTRIB] Failed to store contribution: %v", err)
			}
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// GOSSIPSUB HELPERS (Called from main.go)
// ═══════════════════════════════════════════════════════════════════════════

// CreateGossipSubWithDynamicParams creates GossipSub with cluster-aware parameters
func CreateGossipSubWithDynamicParams(ctx context.Context, h host.Host) (*pubsub.PubSub, error) {
	expectedClusterSize := 8
	if envSize := os.Getenv("CLUSTER_SIZE"); envSize != "" {
		if size, err := strconv.Atoi(envSize); err == nil && size > 0 {
			expectedClusterSize = size
		}
	}

	var D, Dlo, Dhi int

	if expectedClusterSize <= 10 {
		D = expectedClusterSize - 1
		Dlo = max(2, D-1)
		Dhi = expectedClusterSize + 2
	} else if expectedClusterSize <= 50 {
		D = int(math.Sqrt(float64(expectedClusterSize))) + 5
		Dlo = D - 2
		Dhi = D + 5
	} else {
		D = int(math.Log10(float64(expectedClusterSize))) * 10
		Dlo = D - 3
		Dhi = D + 10
	}

	D = max(3, D)
	Dlo = max(2, Dlo)
	Dhi = max(D+2, Dhi)

	messageIDFunc := func(pmsg *pubsub_pb.Message) string {
		h := sha256.New()
		h.Write(pmsg.Data)
		h.Write(pmsg.From)
		return hex.EncodeToString(h.Sum(nil))[:20]
	}

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

	ps, err := pubsub.NewGossipSub(ctx, h,
		pubsub.WithMessageIdFn(messageIDFunc),
		pubsub.WithSeenMessagesTTL(3*time.Minute),
		pubsub.WithFloodPublish(expectedClusterSize <= 10),
		pubsub.WithPeerExchange(true),
		pubsub.WithGossipSubParams(gparams),
		pubsub.WithDirectConnectTicks(5),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create GossipSub: %w", err)
	}

	logger.DISc("✅ GossipSub created: D=%d, Dlo=%d, Dhi=%d (cluster: %d)", D, Dlo, Dhi, expectedClusterSize)
	return ps, nil
}
