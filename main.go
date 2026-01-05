package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/lukesampson/figlet/figletlib"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"math"
	"optimusdb/api"
	"optimusdb/app"
	"optimusdb/config"
	"optimusdb/contextualmetadata"
	"optimusdb/election"
	"optimusdb/logger"
	"optimusdb/utilities"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

func init() {
	app.InitAgentName()
}

// MeshTracer implements pubsub.EventTracer for debugging mesh formation
type MeshTracer struct{}

func (mt *MeshTracer) Trace(evt *pubsub_pb.TraceEvent) {
	if evt == nil || evt.Type == nil {
		return
	}

	switch *evt.Type {
	case pubsub_pb.TraceEvent_GRAFT:
		if evt.Graft != nil && evt.Graft.Topic != nil {
			peerID := "<unknown>"
			if evt.PeerID != nil && len(evt.PeerID) > 0 {
				if pid, err := peer.IDFromBytes(evt.PeerID); err == nil {
					peerID = pid.String()
				}
			}
			logger.Mesh("[MESH] GRAFT: Peer %s joined mesh for topic %s",
				peerID, *evt.Graft.Topic)
		}
	case pubsub_pb.TraceEvent_PRUNE:
		if evt.Prune != nil && evt.Prune.Topic != nil {
			peerID := ""
			if evt.PeerID != nil && len(evt.PeerID) > 0 {
				peerID = fmt.Sprintf("%s", evt.PeerID)
				if len(peerID) > 8 {
					peerID = peerID[:8] + "..."
				}
			}
			logger.Mesh("[MESH] PRUNE: Peer %s left mesh for topic %s",
				peerID, *evt.Prune.Topic)
		}
	case pubsub_pb.TraceEvent_JOIN:
		if evt.Join != nil && evt.Join.Topic != nil {
			logger.Mesh("[MESH] JOIN: Subscribed to topic %s", *evt.Join.Topic)
		}
	case pubsub_pb.TraceEvent_ADD_PEER:
		peerID := ""
		if evt.PeerID != nil && len(evt.PeerID) > 0 {
			peerID = fmt.Sprintf("%s", evt.PeerID)
			if len(peerID) > 8 {
				peerID = peerID[:8] + "..."
			}
		}
		logger.Mesh("[MESH] ADD_PEER: Connected to %s", peerID)
	}
}

// MonitorMeshStatus monitors and logs mesh formation
func MonitorMeshStatus(ctx context.Context, ps *pubsub.PubSub, topic *pubsub.Topic, host host.Host) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			topicPeers := topic.ListPeers()
			allPeers := host.Network().Peers()
			meshPeers := ps.ListPeers("optimusdb")

			logger.Mesh("[MESH-STATUS] Connected: %d, Topic subscribers: %d, Mesh peers: %d",
				len(allPeers), len(topicPeers), len(meshPeers))

			for i, p := range meshPeers {
				shortID := p.String()
				if len(shortID) > 12 {
					shortID = shortID[:12] + "..."
				}
				connectedness := host.Network().Connectedness(p)
				logger.Mesh("[MESH-STATUS]   [%d] %s - %s", i+1, shortID, connectedness)
			}
		}
	}
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func main() {
	flag.Parse()

	printSwarmchestrate()
	utilities.InitMetrics()
	logger.Info("[INFO] Metrics tracking initialized")

	// Metrics (optional)
	if *config.FlagMetrics {
		interval := 2 * time.Second
		if runtime.GOOS == "windows" {
			log.Printf("Running on Windows")
			utilities.GetMemoryUsage()
			_, _, err := utilities.GetDiskUsage(interval)
			if err != nil {
				return
			}
		} else {
			log.Printf("Running on OS: %s", runtime.GOOS)
			utilities.GetMemoryUsage()
			usage, f, f2, err := utilities.GetCPUUsage()
			if err != nil {
				logger.Error("Problem faced in GetCPUUsage, %v %v %v with error: %v", usage, f, f2, err)
				return
			}
			networkUsage, f3, err := utilities.GetNetworkUsage()
			if err != nil {
				logger.Error("Problem faced in GetNetworkUsage, %v %v with error: %v", networkUsage, f3, err)
				return
			}
			diskUsage, f4, err := utilities.GetDiskUsage(interval)
			if err != nil {
				logger.Error("Problem faced in GetDiskUsage, %v %v with error: %v", diskUsage, f4, err)
				return
			}
		}
	}

	// Termination context
	termCtx, termCancel := context.WithCancel(context.Background())

	// Init logging DB
	app.GlobalLoggerDB, _ = app.InitLog()
	logger.SetGlobalDatabase(app.GlobalLoggerDB)

	// Reputation DB
	election.GlobalReputationDB, _ = election.InitReputationDB()

	// OS signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		termCancel()
	}()

	// Optional benchmark monitors
	var bench app.Benchmark
	if *config.FlagBenchmark {
		go app.MonitorMemoryAndCPU(termCtx, &bench)
	}

	// Central logging channel
	logChan := make(chan app.Log, 100)

	// Datastores
	var knowledgeBaseDB app.KnowledgeBaseDB
	var rdbms app.KnowledgeBaseSQLite
	defer rdbms.Close()

	// Init peer + KB components
	if err := app.InitPeer(&knowledgeBaseDB, &rdbms, &bench, logChan); err != nil {
		fprintf, err := fmt.Fprintf(os.Stderr, "Error on setup:\n %+v\n", err)
		if err != nil {
			logger.Error("Problem faced in InitPeer setup, %v with error: %v", fprintf, err)
			return
		}
		logger.Error("Error on setup of InitPeer under main: %v", err)
		os.Exit(1)
	}

	// HostID for payloads/fallbacks
	if knowledgeBaseDB.Node != nil && knowledgeBaseDB.Node.PeerHost != nil {
		knowledgeBaseDB.HostID = string(knowledgeBaseDB.Node.PeerHost.ID())
	}

	// ===============================
	// TINYLLAMA METADATA ENRICHMENT
	// ===============================
	logger.AI("[METADATA] Initializing TinyLlama metadata enrichment...")

	var llmClient *contextualmetadata.HTTPClient
	var metadataEnricher *contextualmetadata.MetadataEnricher

	llmClient, err := contextualmetadata.NewTinyLlamaHTTP()
	if err != nil {
		logger.AI("[METADATA] TinyLlama not available: %v", err)
		logger.AI("[METADATA] Will use basic metadata generation")
		llmClient = nil
	} else {
		healthCtx, healthCancel := context.WithTimeout(termCtx, 20*time.Second)
		if err := llmClient.HealthCheck(healthCtx); err != nil {
			logger.AI("[METADATA] TinyLlama health check failed: %v", err)
			llmClient = nil
		} else {
			logger.AI("[METADATA] TinyLlama client initialized and healthy")
		}
		healthCancel()
	}

	knowledgeBaseDB.MetadataService = &contextualmetadata.Service{
		UseGreek: false,
		Client:   llmClient,
		Saver:    contextualmetadata.OrbitDBSaver{},
	}

	cacheTTL := 24 * time.Hour
	if ttlEnv := os.Getenv("METADATA_CACHE_TTL"); ttlEnv != "" {
		if parsedTTL, err := time.ParseDuration(ttlEnv); err == nil {
			cacheTTL = parsedTTL
		}
	}
	knowledgeBaseDB.MetadataCache = contextualmetadata.NewMetadataCache(cacheTTL)
	logger.AI("[METADATA] Metadata cache initialized (TTL: %v)", cacheTTL)

	if os.Getenv("METADATA_AUTO_ENRICH") == "true" || os.Getenv("METADATA_AUTO_ENRICH") == "1" {
		dbPaths := []string{
			*config.FlagRepo + ".db",
		}

		metadataEnricher = contextualmetadata.NewMetadataEnricher(
			knowledgeBaseDB.MetadataService.(*contextualmetadata.Service),
			&knowledgeBaseDB,
			knowledgeBaseDB.MetadataCache.(*contextualmetadata.MetadataCache),
			dbPaths,
		)

		enrichInterval := 1 * time.Hour
		if intervalEnv := os.Getenv("METADATA_ENRICH_INTERVAL"); intervalEnv != "" {
			if parsedInterval, err := time.ParseDuration(intervalEnv); err == nil {
				enrichInterval = parsedInterval
			}
		}
		metadataEnricher.SetInterval(enrichInterval)

		logger.AI("[METADATA] Background enricher enabled (interval: %v)", enrichInterval)
	} else {
		logger.AI("[METADATA] Background enricher disabled (set METADATA_AUTO_ENRICH=true to enable)")
	}
	logger.AI("[METADATA] Metadata enrichment system initialized")

	// EMS subscriber (ActiveMQ/STOMP)
	emsCtx, emsCancel := context.WithCancel(termCtx)
	cleanupEMS, err := knowledgeBaseDB.StartEMSSubscriber(emsCtx)
	if err != nil {
		log.Printf("[ERROR] EMS init failed: %v", err)
		logger.Error("[ERROR] EMS init failed: %v", err)
	} else {
		go func() {
			<-termCtx.Done()
			_ = cleanupEMS()
			emsCancel()
		}()
		logger.Info("[INFO] EMS service started (auto-reconnect enabled)")
	}

	// API channels
	reqChan := make(chan app.Request, 100)
	resChan := make(chan interface{}, 100)

	// Log fan-in
	go func() {
		for l := range logChan {
			switch l.Type {
			case app.RecoverableErr:
				if err, ok := l.Data.(error); ok {
					logger.Error("[ERROR] Recovering from : %v\n", err)
				} else {
					logger.Error("[ERROR] Recovering from non-error type: %v\n", l.Data)
				}
			case app.NonRecoverableErr:
				if err, ok := l.Data.(error); ok {
					logger.Error("[ERROR] Cannot recover from : %+v\n", err)
				} else {
					logger.Error("[ERROR] Non-recoverable issue, but not an error type: %v\n", l.Data)
				}
				termCancel()
			case app.Info:
				if msg, ok := l.Data.(string); ok {
					logger.Info("[INFO] Logging Channel: %s\n", msg)
				} else {
					logger.Info("[INFO] Unexpected info format: %v\n", l.Data)
				}
			case app.Print:
				log.Print(l.Data)
				logger.Info("[INFO] Main Data: %+v", l.Data)
			default:
				logger.Debug("[DEBUG] Unknown log type: %+v\n", l)
			}
		}
	}()

	// Optional shell
	if *config.FlagShell {
		go api.Shell(reqChan, resChan, logChan)
	}
	// HTTP API
	if *config.FlagHTTP {
		go api.ServeHTTP(&knowledgeBaseDB, app.GlobalLoggerDB, reqChan, resChan, logChan)
	}

	// ===============================
	// USE SINGLE HOST FOR EVERYTHING
	// ===============================
	hostMain := knowledgeBaseDB.Node.PeerHost
	logger.Debug("[DEBUG] Using unified libp2p host for discovery and GossipSub")
	logger.Info("[INFO] Libp2p Agent ID:", hostMain.ID())

	// Register SQL stream handler on main host
	go app.AwaitRegisterSQLDMLStreamHandler(hostMain, logChan)

	// Main service loop on main host
	go app.Service(&knowledgeBaseDB, reqChan, resChan, hostMain, logChan, &rdbms)

	// ===============================
	// PEER DISCOVERY
	// ===============================
	var discoveryService *api.Service

	if *config.FlagAutodiscovery {
		logger.Info("[DISCOVERY] Auto Discovery for Peers has been enabled")

		var prMsg string
		if *config.FlagAutodiscoveryMDNS {
			prMsg = "Using MDNS for Auto-Discovery"
		} else if *config.FlagAutodiscoveryipfsPubSub {
			prMsg = "Using IPFS PubSub for Auto-Discovery"
		} else if *config.FlagAutodiscoveryDHT {
			prMsg = "Using DHT for Auto-Discovery"
		} else {
			prMsg = "No Auto-Discovery method selected"
		}
		logger.DISc("[DISCOVERY] %v", prMsg)

		// Start discovery (discovery service no longer creates GossipSub)
		discoveryService = api.StartDiscovery(hostMain, &knowledgeBaseDB)
		if discoveryService == nil {
			logger.Error("[ERROR] Discovery service failed to start")
		} else {
			logger.DISc("[DISCOVERY] Discovery service started, waiting for peers...")
			time.Sleep(5 * time.Second) // Initial discovery wait
			go api.PrintDiscoveredPeers(&knowledgeBaseDB)
		}
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// DYNAMIC GOSSIPSUB CONFIGURATION (SCALES 3-100+ NODES)
	// ═══════════════════════════════════════════════════════════════════════════
	var ps *pubsub.PubSub
	var electionTopic *pubsub.Topic
	var electionSub *pubsub.Subscription

	logger.Election("════════════════════════════════════════════════════════════")
	logger.Election("INITIALIZING GOSSIPSUB WITH DYNAMIC PARAMETERS")
	logger.Election("════════════════════════════════════════════════════════════")

	// ✅ STEP 1: Determine cluster size (from env or auto-detect)
	expectedClusterSize := 8 // Safe default for small clusters

	if envSize := os.Getenv("CLUSTER_SIZE"); envSize != "" {
		if size, err := strconv.Atoi(envSize); err == nil && size > 0 {
			expectedClusterSize = size
			logger.Election("[CONFIG] Using CLUSTER_SIZE from environment: %d nodes", expectedClusterSize)
		}
	} else {
		// Auto-detect cluster size from discovery
		logger.Election("[CONFIG] CLUSTER_SIZE not set, auto-detecting from discovery...")
		time.Sleep(10 * time.Second) // Wait for discovery to stabilize

		discoveredPeers := knowledgeBaseDB.GetDiscoveredPeers()
		if len(discoveredPeers) > 0 {
			expectedClusterSize = len(discoveredPeers) + 1 // +1 for self
			logger.Election("[CONFIG] Auto-detected cluster size: %d nodes (discovered: %d + self: 1)",
				expectedClusterSize, len(discoveredPeers))
		} else {
			logger.Election("[CONFIG] No peers discovered yet, using default: %d nodes", expectedClusterSize)
		}
	}

	// ✅ STEP 2: Calculate optimal GossipSub parameters based on cluster size
	var D, Dlo, Dhi int
	var meshType string

	if expectedClusterSize <= 10 {
		// FULL MESH for small clusters (3-10 nodes)
		// Every node connects to every other node for maximum reliability
		D = expectedClusterSize - 1
		Dlo = max(2, D-1)
		Dhi = expectedClusterSize + 2
		meshType = "FULL MESH"
		logger.Election("[CONFIG] Small cluster detected: configuring for FULL MESH")
	} else if expectedClusterSize <= 50 {
		// PARTIAL MESH for medium clusters (11-50 nodes)
		// Each node connects to sqrt(N) + 5 peers for good coverage
		D = int(math.Sqrt(float64(expectedClusterSize))) + 5
		Dlo = D - 2
		Dhi = D + 5
		meshType = "PARTIAL MESH"
		logger.Election("[CONFIG] Medium cluster detected: configuring for PARTIAL MESH")
	} else {
		// SPARSE MESH for large clusters (51+ nodes)
		// Each node connects to log10(N) * 10 peers for efficiency
		D = int(math.Log10(float64(expectedClusterSize))) * 10
		Dlo = D - 3
		Dhi = D + 10
		meshType = "SPARSE MESH"
		logger.Election("[CONFIG] Large cluster detected: configuring for SPARSE MESH")
	}

	// Apply safety bounds
	D = max(3, D)
	Dlo = max(2, Dlo)
	Dhi = max(D+2, Dhi)

	logger.Election("[CONFIG] ════════════════════════════════════════════════════════════")
	logger.Election("[CONFIG] Mesh Configuration:")
	logger.Election("[CONFIG]   Type: %s", meshType)
	logger.Election("[CONFIG]   Cluster size: %d nodes", expectedClusterSize)
	logger.Election("[CONFIG]   D (target peers): %d", D)
	logger.Election("[CONFIG]   Dlo (minimum): %d", Dlo)
	logger.Election("[CONFIG]   Dhi (maximum): %d", Dhi)
	logger.Election("[CONFIG]   Expected coverage: %.1f%%", (float64(D)/float64(expectedClusterSize-1))*100)
	logger.Election("[CONFIG] ════════════════════════════════════════════════════════════")

	// ✅ STEP 3: Create message ID function for deduplication
	messageIDFunc := func(pmsg *pubsub_pb.Message) string {
		h := sha256.New()
		h.Write(pmsg.Data)
		h.Write(pmsg.From)
		return hex.EncodeToString(h.Sum(nil))[:20]
	}

	// ✅ STEP 4: Configure GossipSub parameters
	gparams := pubsub.DefaultGossipSubParams()
	gparams.D = D                                 // ✅ DYNAMIC - scales with cluster
	gparams.Dlo = Dlo                             // ✅ DYNAMIC - scales with cluster
	gparams.Dhi = Dhi                             // ✅ DYNAMIC - scales with cluster
	gparams.Dscore = max(2, D/2)                  // Peer score threshold
	gparams.Dout = max(2, D/3)                    // Outbound connections
	gparams.Dlazy = max(3, D/2)                   // Gossip peers
	gparams.HeartbeatInterval = 1 * time.Second   // Heartbeat frequency
	gparams.HistoryLength = 12                    // Message history
	gparams.HistoryGossip = 6                     // Gossip history
	gparams.GossipFactor = 0.3                    // Gossip probability
	gparams.OpportunisticGraftTicks = 40          // Opportunistic grafting
	gparams.OpportunisticGraftPeers = 3           // Opportunistic peers
	gparams.PruneBackoff = 15 * time.Second       // Prune backoff
	gparams.GraftFloodThreshold = 3 * time.Second // Graft flood threshold
	gparams.FanoutTTL = 45 * time.Second          // Fanout TTL

	// ✅ STEP 5: Build GossipSub options
	psOpts := []pubsub.Option{
		pubsub.WithMessageIdFn(messageIDFunc),
		pubsub.WithSeenMessagesTTL(3 * time.Minute),
		pubsub.WithFloodPublish(expectedClusterSize <= 10), // ✅ DYNAMIC - only for small clusters
		pubsub.WithPeerExchange(true),
		pubsub.WithDirectPeers([]peer.AddrInfo{}),
		pubsub.WithGossipSubParams(gparams),
		pubsub.WithDirectConnectTicks(5),
		pubsub.WithEventTracer(&MeshTracer{}),
	}

	// Optional: Enable JSON trace logging
	if trace := os.Getenv("GOSSIPSUB_TRACE"); trace != "" {
		if tr, err := pubsub.NewJSONTracer(trace); err == nil {
			psOpts = append(psOpts, pubsub.WithEventTracer(tr))
			logger.Debug("[GOSSIPSUB] Trace logging enabled: %s", trace)
		}
	}

	// ✅ STEP 6: Create GossipSub instance with better peer scoring
	psOpts = append(psOpts, election.CreateBetterGossipSubParams())
	ps, err = pubsub.NewGossipSub(termCtx, hostMain, psOpts...)
	if err != nil {
		logger.Error("[FATAL] Failed to initialize GossipSub: %v", err)
		os.Exit(1)
	}
	logger.Election("[GOSSIPSUB] Successfully created with D=%d, Dlo=%d, Dhi=%d", D, Dlo, Dhi)

	// ✅ STEP 7: Join election topic
	electionTopic, err = ps.Join("optimusdb")
	if err != nil {
		logger.Error("[FATAL] Failed to join election topic: %v", err)
		os.Exit(1)
	}
	logger.Election("[GOSSIPSUB] Joined topic 'optimusdb'")

	// ✅ STEP 8: Subscribe to election topic
	electionSub, err = electionTopic.Subscribe()
	if err != nil {
		logger.Error("[FATAL] Failed to subscribe to election topic: %v", err)
		os.Exit(1)
	}
	logger.Election("[GOSSIPSUB] Subscribed to topic 'optimusdb'")

	// ✅ STEP 9: Store in knowledgeBaseDB for election to use
	knowledgeBaseDB.ElectionTopic = electionTopic
	knowledgeBaseDB.ElectionSub = electionSub
	knowledgeBaseDB.PubSub = ps

	logger.Election("════════════════════════════════════════════════════════════")
	logger.Election("GOSSIPSUB INITIALIZATION COMPLETE")
	logger.Election("  Scales from 3 to 100+ nodes automatically")
	logger.Election("  Current configuration: %s for %d nodes", meshType, expectedClusterSize)
	logger.Election("════════════════════════════════════════════════════════════")

	// ===============================
	// START MESH MONITORING
	// ===============================
	go MonitorMeshStatus(termCtx, ps, electionTopic, hostMain)

	// ═══════════════════════════════════════════════════════════════
	// MESH STABILIZATION WITH PROGRESSIVE VERIFICATION
	// ═══════════════════════════════════════════════════════════════
	logger.Election("[MESH] Waiting for mesh formation...")
	time.Sleep(10 * time.Second) // Initial wait for discovery

	maxMeshWaitAttempts := 6
	meshCheckInterval := 5 * time.Second
	requiredMeshCoverage := 0.8 // Require 80% mesh coverage

	for attempt := 1; attempt <= maxMeshWaitAttempts; attempt++ {
		discoveredPeers := knowledgeBaseDB.GetDiscoveredPeers()
		meshPeers := electionTopic.ListPeers()
		connectedPeers := hostMain.Network().Peers()

		discoveredCount := len(discoveredPeers)
		meshCount := len(meshPeers)
		connectedCount := len(connectedPeers)

		logger.Election("[MESH] Check %d/%d: discovered=%d, connected=%d, mesh=%d",
			attempt, maxMeshWaitAttempts, discoveredCount, connectedCount, meshCount)

		// Calculate mesh coverage
		var meshCoverage float64
		if discoveredCount > 0 {
			meshCoverage = float64(meshCount) / float64(discoveredCount)
		} else {
			meshCoverage = 0
		}

		logger.Election("[MESH]   Coverage: %.1f%% (target: %.1f%%)", meshCoverage*100, requiredMeshCoverage*100)

		// Check if mesh is sufficiently formed
		if meshCoverage >= requiredMeshCoverage && meshCount >= 2 {
			logger.Election("[MESH] Stabilization COMPLETE - Coverage: %.1f%%", meshCoverage*100)
			break
		}

		if attempt < maxMeshWaitAttempts {
			logger.Warn("[MESH] Coverage insufficient, waiting %v...", meshCheckInterval)
			time.Sleep(meshCheckInterval)
		} else {
			logger.Warn("[MESH] Stabilization incomplete after %d attempts", maxMeshWaitAttempts)
			logger.Warn("[MESH]   Proceeding with partial mesh (%.1f%% coverage)", meshCoverage*100)
			logger.Warn("[MESH]   Elections may have reduced reliability")
		}
	}

	// Final verification
	finalMeshPeers := electionTopic.ListPeers()
	finalDiscovered := knowledgeBaseDB.GetDiscoveredPeers()
	logger.Election("[MESH] Final status: mesh=%d peers, discovered=%d peers",
		len(finalMeshPeers), len(finalDiscovered))

	// Additional stabilization buffer
	time.Sleep(5 * time.Second)

	// ===============================
	// START ELECTION CONTROLLER
	// ===============================
	electionNode := election.RunFullNode(termCtx, hostMain, ps, &knowledgeBaseDB)
	logger.Election("[ELECTION] Controller initialized (node stored globally: %v)", electionNode != nil)

	// ===============================
	// START BACKGROUND METADATA ENRICHER
	// ===============================
	if metadataEnricher != nil {
		logger.Info("[METADATA] Starting background metadata enricher...")
		metadataEnricher.Start()

		// Trigger initial scan after cluster stabilizes
		go func() {
			time.Sleep(30 * time.Second)
			logger.Info("[METADATA] Triggering initial enrichment scan...")
			metadataEnricher.EnrichNow()
		}()

		logger.Info("[METADATA] Background enricher started")
	}

	// Register shutdown handlers
	if discoveryService != nil {
		go handleShutdown(discoveryService, &knowledgeBaseDB, hostMain)
	}

	if metadataEnricher != nil {
		go func() {
			<-termCtx.Done()
			logger.Info("[METADATA] Stopping metadata enricher...")
			metadataEnricher.Stop()
			logger.Info("[METADATA] Metadata enricher stopped")
		}()
	}

	// Await termination
	<-termCtx.Done()
	logger.Info("[SHUTDOWN] Shutting down OptimusDB node...")

	// Persist config & benchmark
	err = config.SaveStructAsJSON(knowledgeBaseDB.Config, *config.FlagRepo+"_config")
	if err != nil {
		logger.Error("Problem saving config: %v", err)
		return
	}
	benchmarkPath := *config.FlagRepo + "_benchmark"
	err = config.SaveStructAsJSON(knowledgeBaseDB.Benchmark, benchmarkPath)
	if err != nil {
		logger.Error("Problem saving benchmark: %v", err)
		return
	}

	// Close OrbitDB
	if knowledgeBaseDB.Orbit != nil {
		err := (*knowledgeBaseDB.Orbit).Close()
		if err != nil {
			logger.Error("Problem closing OrbitDB: %v", err)
			return
		}
	}

	log.Println("[SHUTDOWN] Complete")
	logger.Info("[SHUTDOWN] OptimusDB node shutdown complete")
}

func printSwarmchestrate() {
	fontsDir := "/usr/share/figlet/fonts"
	var font *figletlib.Font

	if _, err := os.Stat(fontsDir); os.IsNotExist(err) {
		logger.Debug("Directory does not exist: %v, will create", fontsDir)
		fontsDir = figletlib.GuessFontsDirectory()
		f, err := figletlib.GetFontByName(fontsDir, "standard")
		if err != nil {
			logger.Warn("Could not load figlet fonts: %v", err)
			return
		}
		font = f
	} else {
		logger.Info("Directory exists: %v", fontsDir)
		f, err := figletlib.GetFontByName(fontsDir, "standard")
		if err != nil {
			logger.Warn("Could not load figlet fonts: %v", err)
			return
		}
		font = f
	}

	figletlib.PrintMsg("Swarmchestrate", font, 80, font.Settings(), "")
	figletlib.PrintMsg("ICCS", font, 40, font.Settings(), "")
}

func handleShutdown(service *api.Service, knowledgeBaseDB *app.KnowledgeBaseDB, h host.Host) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	logger.Info("[SHUTDOWN] Received shutdown signal...")
	if service != nil {
		service.StopDiscovery()
		logger.Info("[SHUTDOWN] Peer Discovery stopped.")
	}

	if knowledgeBaseDB != nil && knowledgeBaseDB.Orbit != nil {
		err := (*knowledgeBaseDB.Orbit).Close()
		if err != nil {
			logger.Error("Problem closing OrbitDB: %v", err)
			return
		}
		logger.Info("[SHUTDOWN] OrbitDB instance closed.")
	}

	if err := h.Close(); err != nil {
		logger.Error("[SHUTDOWN] Error closing LibP2P host: %v", err)
	} else {
		logger.Info("[SHUTDOWN] LibP2P host closed successfully.")
	}

	os.Exit(0)
}

func shutdownHandler(fn func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for {
			switch <-ch {
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				fn()
				os.Exit(0)
			}
		}
	}()
}
