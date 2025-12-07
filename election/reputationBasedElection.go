package election

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"optimusdb/logger"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"optimusdb/app"
	"optimusdb/config"
	"optimusdb/utilities"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

var GlobalReputationDB *ReputationSQLite
var GlobalElectionNode *Node
var electionNodeMutex sync.RWMutex

type ReputationSQLite struct {
	ReputationDB *sql.DB
	mu           sync.Mutex
}

type TopicManager struct {
	pubsub *pubsub.PubSub
	topics map[string]*pubsub.Topic
	subs   map[string]*pubsub.Subscription
	mu     sync.Mutex
}

func NewTopicManager(ps *pubsub.PubSub) *TopicManager {
	return &TopicManager{
		pubsub: ps,
		topics: make(map[string]*pubsub.Topic),
		subs:   make(map[string]*pubsub.Subscription),
	}
}

func (tm *TopicManager) GetTopicAndSubscribe(name string) (*pubsub.Topic, *pubsub.Subscription, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Get or create topic
	topic, ok := tm.topics[name]
	if !ok {
		logger.Error("[ERROR] failed Creating new ELECTION topic: %s", name)
		var err error
		topic, err = tm.pubsub.Join(name)
		if err != nil {
			logger.Error("[ERROR] failed to join ELECTION topic '%s': %w", name, err)
			return nil, nil, fmt.Errorf("failed to join topic '%s': %w", name, err)
		}
		tm.topics[name] = topic
	} else {
		logger.Info("[ELECTION] Reusing existing topic: %s", name)
	}

	// Get or create subscription
	sub, ok := tm.subs[name]
	if !ok {
		logger.Info("[ELECTION] Creating new subscription for: %s", name)
		var err error
		sub, err = topic.Subscribe()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to subscribe to topic '%s': %w", name, err)
		}
		tm.subs[name] = sub
	}

	return topic, sub, nil
}

// Constants
const (
	electionTopic = "optimusdb"

	TypeVote           = "vote"
	TypeHeartbeat      = "heartbeat"
	TypeRole           = "role"
	TypeAnnouncement   = "announcement"
	TypeReputation     = "reputation"
	TypeElectionResult = "election_result"

	heartbeatInterval      = 5 * time.Second
	heartbeatTimeout       = 15 * time.Second
	electionTimeout        = 10 * time.Second
	peerDiscoveryThreshold = 1
	reElectionBackoff      = 15 * time.Second
	heartbeatRetryLimit    = 3

	PhaseIdle      = "idle"
	PhaseVoting    = "voting"
	PhaseCompleted = "completed"
)

// Message types
type CoreMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type ElectionResultMessage struct {
	LeaderID string         `json:"leader"`
	Votes    map[string]int `json:"votes"`
	Term     int            `json:"term"`
}

type NodeReputation struct {
	NodeID                string  `json:"nodeId"`
	Uptime                float64 `json:"uptime"`
	LeadershipCount       int     `json:"leadership_count"`
	Latency               float64 `json:"latency"`
	UserCPU               float64 `json:"user_cpu"`
	SystemCPU             float64 `json:"system_cpu"`
	IdleCPU               float64 `json:"idle_cpu"`
	MemoryAvailable       float64 `json:"memory_available"`
	MemoryAllocationTotal float64 `json:"memory_total_alloc"`
	MemorySystem          float64 `json:"memory_sys"`
	AvgReadMBs            float64 `json:"avg_read_mbs"`
	AvgWriteMBs           float64 `json:"avg_write_mbs"`
	GeographyScore        float64 `json:"geography_score"`
}

type VoteMessage struct {
	NodeID     string `json:"nodeId"`
	Vote       string `json:"vote"`
	ElectionID string `json:"electionId"`
	Term       int    `json:"term"`
}

type HeartbeatMessage struct {
	LeaderID string `json:"leaderId"`
	Time     int64  `json:"time"`
	Term     int    `json:"term"`
}

type RoleMessage struct {
	NodeID string `json:"nodeId"`
	Role   string `json:"role"`
	Term   int    `json:"term"`
}

// Node state
type Node struct {
	ctx             context.Context
	host            host.Host
	pubsub          *pubsub.PubSub
	topicManager    *TopicManager
	leader          peer.ID
	mutex           sync.Mutex
	lastHeartbeat   time.Time
	heartbeatMissed int
	role            string
	discovery       *app.KnowledgeBaseDB
	electionTopic   *pubsub.Topic
	electionSub     *pubsub.Subscription
	leadershipCount int

	votes                      map[string]int
	votedNodes                 map[string]string
	currentElectionID          string
	electionMutex              sync.Mutex
	isElecting                 int32
	lastElection               time.Time
	announcedLeaderForElection map[string]string
	announcementMutex          sync.Mutex

	currentTerm      int
	votedForInTerm   map[int]string
	electionPhase    string
	electionDeadline time.Time
	listenerStarted  int32
	electionCancel   context.CancelFunc
	peerCount        int
}

// Reputation weights
func getReputationWeights() map[string]float64 {
	return map[string]float64{
		"uptime":          0.20,
		"leadership":      0.10,
		"cpu":             0.20,
		"memory":          0.20,
		"disk":            0.10,
		"latency":         0.10,
		"geography_score": 0.10,
	}
}

// DB initialization
func InitReputationDB() (*ReputationSQLite, error) {
	rdbmsCache := filepath.Join(filepath.Join(filepath.Join(os.Getenv("HOME"), ".cache"), "optimusdb", *config.FlagRepo, "optimusdb"), "optimusreputation.db")
	dir := filepath.Dir(rdbmsCache)
	if err := os.MkdirAll(dir, 0755); err != nil {
		logger.Error("[ERROR] failed to create directory for Reputation DB: %w", err)
		return nil, fmt.Errorf("failed to create directory for Reputation DB: %w", err)
	}
	db, err := sql.Open("sqlite3", rdbmsCache)
	if err != nil {
		logger.Error("[ERROR] Cannot open SQLite DB for Reputation mechanism: %v", err)
		//log.Fatalf("[FATAL] Cannot open SQLite DB: %v", err)
	}
	GlobalReputationDB = &ReputationSQLite{ReputationDB: db}
	if err := GlobalReputationDB.createReputationDB(); err != nil {
		logger.Error("[ERROR] Table creation failed for Reputation DB: %v", err)
		//log.Fatalf("[ERROR] Table creation failed for Reputation DB: %v", err)
		return nil, err
	}
	logger.Info("[ELECTION] SQLite Reputation Database Ready at:", rdbmsCache)
	return GlobalReputationDB, nil
}

func (rep *ReputationSQLite) createReputationDB() error {
	tableQuery := `CREATE TABLE IF NOT EXISTS reputation (
		node_id TEXT PRIMARY KEY,
		uptime REAL,
		leadership_count INTEGER,
		latency REAL,
		user_cpu REAL,
		system_cpu REAL,
		idle_cpu REAL,
		memory_available REAL,
		memory_total_alloc REAL,
		memory_sys REAL,
		avg_read_mbs REAL,
		avg_write_mbs REAL,
		geography_score REAL
	);`
	if _, err := rep.ReputationDB.Exec(tableQuery); err != nil {
		return err
	}

	electionLogQuery := `CREATE TABLE IF NOT EXISTS election_log (
		id TEXT PRIMARY KEY,
		timestamp TEXT,
		leader_id TEXT,
		term INTEGER,
		votes_json TEXT
	);`
	if _, err := rep.ReputationDB.Exec(electionLogQuery); err != nil {
		logger.Error("[ERROR] failed to create election_log table: %w", err)
		return fmt.Errorf("failed to create election_log table: %w", err)
	}

	return nil
}

func calculateReputation(nr NodeReputation) float64 {
	w := getReputationWeights()

	// ✅ CPU Score: 0-100 (lower usage = better, cap at 100%)
	cpuUsage := nr.UserCPU + nr.SystemCPU
	if cpuUsage > 100 {
		cpuUsage = 100
	}
	cpuScore := 100 - cpuUsage

	// ✅ Memory Score: 0-100 (less used = better, as percentage of system memory)
	memoryScore := 100.0
	if nr.MemorySystem > 0 {
		memoryUsedPct := (nr.MemoryAllocationTotal / nr.MemorySystem) * 100
		if memoryUsedPct > 100 {
			memoryUsedPct = 100
		}
		memoryScore = 100 - memoryUsedPct
	}

	// ✅ Disk Score: Normalize with logarithmic scale to handle bursts gracefully
	// This handles values from 0.1 MB/s (score ~100) to 10,000 MB/s (score ~0)
	diskIO := nr.AvgReadMBs + nr.AvgWriteMBs
	diskScore := 100.0
	if diskIO > 0 {
		// Log scale: 1 MB/s = 100, 10 MB/s = 75, 100 MB/s = 50, 1000 MB/s = 25, 10000+ MB/s = 0
		// Formula: 100 - (log10(diskIO) * 25)
		logDisk := math.Log10(diskIO)
		diskScore = 100 - (logDisk * 25)

		// Clamp to 0-100
		if diskScore < 0 {
			diskScore = 0
		}
		if diskScore > 100 {
			diskScore = 100
		}
	}

	// ✅ Latency Score: 0-100 (lower latency = better, assume max 100ms)
	latency := nr.Latency
	if latency > 100 {
		latency = 100
	}
	latencyScore := 100 - latency

	// ✅ Uptime Score: Convert 0-1 range to 0-100
	uptimeScore := nr.Uptime * 100
	if uptimeScore > 100 {
		uptimeScore = 100
	}

	// ✅ Leadership Score: Each past leadership worth 10 points, capped at 100
	leadershipScore := float64(nr.LeadershipCount) * 10
	if leadershipScore > 100 {
		leadershipScore = 100
	}

	// ✅ Geography Score: Convert 0-1 range to 0-100
	geographyScore := nr.GeographyScore * 100
	if geographyScore > 100 {
		geographyScore = 100
	}

	// Calculate weighted sum
	score := (w["uptime"] * uptimeScore) +
		(w["leadership"] * leadershipScore) +
		(w["cpu"] * cpuScore) +
		(w["memory"] * memoryScore) +
		(w["disk"] * diskScore) +
		(w["latency"] * latencyScore) +
		(w["geography_score"] * geographyScore)

	// ✅ Final safety clamp (should not be needed, but just in case)
	if score < 0 {
		return 0
	}
	if score > 100 {
		return 100
	}

	return score
}

// IMPROVED publish with better debugging
func (n *Node) publishMessage(msgType string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		logger.Error("[ERROR] marshal payload failed for ELECTION: %w", err)
		return fmt.Errorf("marshal payload failed: %w", err)
	}

	core := CoreMessage{Type: msgType, Payload: data}
	coreData, err := json.Marshal(core)
	if err != nil {
		logger.Error("[ERROR] marshal CoreMessage failed for ELECTION: %w", err)
		return fmt.Errorf("marshal core failed: %w", err)
	}

	// Check mesh status before publishing
	meshPeers := n.electionTopic.ListPeers()
	logger.Info("[ELECTION] ELECTION Publish, Check mesh status before publishing, Type: %s, Size: %d bytes, Mesh peers: %d",
		msgType, len(coreData), len(meshPeers))

	if len(meshPeers) == 0 {
		logger.Info("[ELECTION] No mesh peers! Message may not propagate")

		// List all connected peers for comparison
		allPeers := n.host.Network().Peers()
		logger.Info("[ELECTION] ELECTION, List all connected peers for comparison, Connected peers: %d", len(allPeers))

		// Check subscription status
		topics := n.pubsub.GetTopics()
		logger.Info("[ELECTION] ELECTION,Our subscribed topics: %v", topics)
	}

	// Publish with retries
	for attempt := 0; attempt < 3; attempt++ {
		err = n.electionTopic.Publish(n.ctx, coreData)
		if err == nil {
			logger.Info("[ELECTION] ELECTION Publish ✅ %s published (attempt %d)", msgType, attempt+1)
			return nil
		}

		logger.Error("[ERROR] ELECTION Publish ⚠️ Attempt %d failed: %v", attempt+1, err)
		if attempt < 2 {
			time.Sleep(500 * time.Millisecond)
		}
	}

	return fmt.Errorf("failed after 3 attempts: %w", err)
}

// StartElection with better coordination
func (n *Node) StartElection(peers []NodeReputation, attempt int) {
	if !atomic.CompareAndSwapInt32(&n.isElecting, 0, 1) {
		logger.Info("[ELECTION] ELECTION Already in progress, skipping")
		return
	}
	defer atomic.StoreInt32(&n.isElecting, 0)

	discoveredPeers := n.discovery.GetDiscoveredPeers()
	totalPeers := len(discoveredPeers) + 1

	n.electionMutex.Lock()
	n.currentTerm++
	term := n.currentTerm
	n.peerCount = totalPeers
	n.electionMutex.Unlock()

	//logger.Info("[ELECTION] ════════════════════════════════════════")
	logger.Info("[ELECTION] Starting Term %d, Attempt %d", term, attempt+1)
	logger.Info("[ELECTION] Discovered: %d, Total cluster: %d", len(discoveredPeers), totalPeers)
	logger.Info("[ELECTION] Topic peers: %d", len(n.electionTopic.ListPeers()))
	//logger.Info("[ELECTION] ════════════════════════════════════════")

	// Generate election ID
	electionID := fmt.Sprintf("%s-term%d-%d-attempt%d",
		n.host.ID().String(), term, time.Now().UnixNano(), attempt)

	n.electionMutex.Lock()
	n.currentElectionID = electionID
	n.electionPhase = PhaseVoting
	n.electionDeadline = time.Now().Add(electionTimeout)
	n.votes = make(map[string]int)
	n.votedNodes = make(map[string]string)
	n.electionMutex.Unlock()

	// Ensure we have candidates
	if len(peers) == 0 {
		peers = []NodeReputation{{NodeID: string(n.host.ID())}}
	}

	// Select and vote
	selected := n.selectCandidate(peers)
	vote := VoteMessage{
		NodeID:     string(n.host.ID()),
		Vote:       selected,
		ElectionID: electionID,
		Term:       term,
	}

	// Record own vote immediately
	n.electionMutex.Lock()
	n.votedNodes[vote.NodeID] = vote.Vote
	n.votes[vote.Vote]++
	n.electionMutex.Unlock()

	// Publish vote
	if err := n.publishMessage(TypeVote, vote); err != nil {
		logger.Info("[ERROR] Failed to publish vote: %v", err)
	}

	logger.Info("[ELECTION] Node %s voted for %s",
		vote.NodeID,
		vote.Vote)

	// Wait for votes
	electionCtx, cancel := context.WithTimeout(n.ctx, electionTimeout)
	defer cancel()

	n.electionMutex.Lock()
	n.electionCancel = cancel
	n.electionMutex.Unlock()

	<-electionCtx.Done()
	n.finalizeElection(term, electionID, attempt, peers)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (n *Node) selectCandidate(peers []NodeReputation) string {
	if len(peers) == 0 {
		return string(n.host.ID())
	}

	// Weight-based selection
	total := 0.0
	for _, p := range peers {
		total += calculateReputation(p)
	}

	if total <= 0 {
		return peers[rand.Intn(len(peers))].NodeID
	}

	randVal := rand.Float64() * total
	cumulative := 0.0
	for _, p := range peers {
		cumulative += calculateReputation(p)
		if cumulative >= randVal {
			return p.NodeID
		}
	}

	return peers[len(peers)-1].NodeID
}

// RELAXED quorum for small networks
func (n *Node) finalizeElection(term int, electionID string, attempt int, peers []NodeReputation) {
	n.electionMutex.Lock()
	if n.currentElectionID != electionID || n.currentTerm != term {
		n.electionMutex.Unlock()
		return
	}
	n.electionPhase = PhaseCompleted

	logger.Info("[ELECTION] ════════════════════════════════════════")
	logger.Info("[ELECTION] Results for Term %d:", term)
	for candidate, count := range n.votes {
		logger.Info("[ELECTION]   %s: %d votes", candidate, count)
	}
	logger.Info("[ELECTION] Participation: %d/%d nodes voted", len(n.votedNodes), n.peerCount)
	logger.Info("[ELECTION] ════════════════════════════════════════")

	winner := n.determineWinner()
	votesCopy := make(map[string]int)
	for k, v := range n.votes {
		votesCopy[k] = v
	}
	n.electionMutex.Unlock()

	if winner == "" {
		logger.Info("[ELECTION] No winner, attempt %d/%d", attempt+1, 3)
		if attempt < 2 {
			time.Sleep(time.Duration(math.Pow(2, float64(attempt))) * time.Second)
			n.StartElection(peers, attempt+1)
		} else {
			n.fallbackElection()
		}
		return
	}

	logger.Info("[ELECTION] WINNER: %s with %d votes", winner, votesCopy[winner])
	n.announceLeader(winner, term)
}

// RELAXED winner determination for Docker
func (n *Node) determineWinner() string {
	if len(n.votes) == 0 {
		return ""
	}

	var winner string
	maxVotes := 0
	for node, count := range n.votes {
		if count > maxVotes || (count == maxVotes && node < winner) {
			maxVotes = count
			winner = node
		}
	}

	// Very relaxed: accept any winner with votes
	participation := len(n.votedNodes)
	required := 1

	if n.peerCount <= 3 {
		required = 1 // Small cluster: any vote wins
	} else if n.peerCount <= 8 {
		required = 2 // Medium: need 2 votes
	} else {
		required = (n.peerCount * 3) / 10 // Large: 30%
	}

	logger.Info("[ELECTION] Participation: %d, Required: %d", participation, required)

	if participation >= required && maxVotes >= 1 {
		return winner
	}

	return ""
}

// ENHANCED listener with detailed logging
func (n *Node) ListenForElectionEvents() {
	if !atomic.CompareAndSwapInt32(&n.listenerStarted, 0, 1) {
		return
	}

	logger.Info("[LISTENER] ════════════════════════════════════")
	logger.Info("[LISTENER] Starting election listener")
	logger.Info("[LISTENER] Node: %s", n.host.ID().String())
	logger.Info("[LISTENER] ════════════════════════════════════")

	if n.electionSub == nil {
		log.Fatal("[LISTENER] No subscription!")
	}

	go func() {
		msgCount := 0
		for {
			msg, err := n.electionSub.Next(n.ctx)
			if err != nil {
				if n.ctx.Err() != nil {
					return
				}
				logger.Info("[ERROR] Receive failed: %v", err)
				continue
			}

			msgCount++
			//from := string(msg.ReceivedFrom)// not correct
			from := fmt.Sprintf("%s", msg.ReceivedFrom)
			if len(from) > 8 {
				from = from[:8] + "..."
			}

			logger.Info("[ELECTION] MSG-RX-%d From: %s, Size: %d bytes", msgCount, from, len(msg.Data))

			var core CoreMessage
			if err := json.Unmarshal(msg.Data, &core); err != nil {
				logger.Info("[ERROR] Unmarshal failed: %v", err)
				continue
			}

			logger.Info("[ELECTION] MSG-RX-%d Type: %s", msgCount, core.Type)
			n.handleMessage(core, msg.ReceivedFrom)
		}
	}()
}

// Message handler
func (n *Node) handleMessage(core CoreMessage, from peer.ID) {
	switch core.Type {
	case TypeVote:
		var vote VoteMessage
		if err := json.Unmarshal(core.Payload, &vote); err != nil {
			return
		}

		logger.Info("[ELECTION] VOTE-RX %s voted for %s (election: %s, term: %d)",
			vote.NodeID,
			vote.Vote,
			vote.ElectionID,
			vote.Term,
		)

		n.handleVote(vote)

	case TypeHeartbeat:
		var hb HeartbeatMessage
		if err := json.Unmarshal(core.Payload, &hb); err != nil {
			return
		}
		logger.Info("[ELECTION] HB-RX From %s (term %d)", hb.LeaderID, hb.Term)
		n.handleHeartbeat(hb)

	case TypeReputation:
		var rep NodeReputation
		if err := json.Unmarshal(core.Payload, &rep); err != nil {
			return
		}
		hostid := n.host.ID().String() // Use .String() method
		if rep.NodeID != hostid {
			logger.Info("[ELECTION] REP-RX From %s, Score: %.2f", rep.NodeID, calculateReputation(rep))
			UpsertReputation(GlobalReputationDB.ReputationDB, rep)
		}

	case TypeAnnouncement:
		var ann map[string]interface{}
		if err := json.Unmarshal(core.Payload, &ann); err != nil {
			return
		}
		leaderID, _ := ann["leader"].(string)
		term := int(ann["term"].(float64))

		logger.Info("[ELECTION] ANNOUNCE-RX Leader: %s (term %d)", leaderID, term)
		n.handleAnnouncement(leaderID, term)

	case TypeElectionResult:
		var result ElectionResultMessage
		if err := json.Unmarshal(core.Payload, &result); err != nil {
			return
		}
		logger.Info("[ELECTION] RESULT-RX Leader: %s, Term: %d, Votes: %v",
			result.LeaderID, result.Term, result.Votes)
	}
}

func (n *Node) handleVote(vote VoteMessage) {
	n.electionMutex.Lock()
	defer n.electionMutex.Unlock()

	// Join election if idle
	if n.electionPhase == PhaseIdle {
		n.electionPhase = PhaseVoting
		n.currentElectionID = vote.ElectionID
		n.currentTerm = vote.Term
		n.electionDeadline = time.Now().Add(electionTimeout)
		n.votes = make(map[string]int)
		n.votedNodes = make(map[string]string)
	}

	// Validate
	if n.electionPhase != PhaseVoting ||
		vote.ElectionID != n.currentElectionID ||
		vote.Term != n.currentTerm {
		return
	}

	// Record vote
	if _, hasVoted := n.votedNodes[vote.NodeID]; !hasVoted {
		n.votedNodes[vote.NodeID] = vote.Vote
		n.votes[vote.Vote]++

		logger.Info("[ELECTION] Recorded: %s → %s (total: %d)",
			vote.NodeID, //
			vote.Vote,   //
			n.votes[vote.Vote])
	}
}

// WRONG: peer.ID(stringVariable)
// This treats the base58 string as raw bytes, causing corruption!
// CORRECT: peer.Decode(stringVariable)
// This properly decodes the base58-encoded string back to peer.ID
func (n *Node) handleHeartbeat(hb HeartbeatMessage) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	// ✅ Coordinators now detect split-brain
	if n.role == "Coordinator" {
		if hb.LeaderID != n.host.ID().String() {
			// Another coordinator exists!
			if hb.Term > n.currentTerm {
				n.stepDownLocked(hb.LeaderID, hb.Term) // ✅ Step down
			} else if hb.Term == n.currentTerm && hb.LeaderID < n.host.ID().String() {
				n.stepDownLocked(hb.LeaderID, hb.Term) // ✅ Tiebreaker
			}
		}
		return
	}

	// Follower logic
	if n.role == "Follower" {
		n.lastHeartbeat = time.Now()
		n.heartbeatMissed = 0
		leaderPeerID, err := peer.Decode(hb.LeaderID)
		if err != nil {
			return
		}
		n.leader = leaderPeerID
		if hb.Term > n.currentTerm {
			n.currentTerm = hb.Term
		}
	}
}

func (n *Node) stepDownLocked(newLeaderID string, term int) {
	logger.Info("[ELECTION] ⬇️ STEPPING DOWN from Coordinator to Follower")
	n.role = "Follower"
	n.currentTerm = term
	n.lastHeartbeat = time.Now()
	n.heartbeatMissed = 0
	leaderPeerID, _ := peer.Decode(newLeaderID)
	n.leader = leaderPeerID
}

func (n *Node) handleAnnouncement(leaderID string, term int) {
	n.mutex.Lock()

	// Convert string to peer.ID properly
	leaderPeerID, err := peer.Decode(leaderID)
	if err != nil {
		logger.Info("[ERROR] Failed to decode leader ID: %v", err)
		n.mutex.Unlock()
		return
	}

	if leaderID == n.host.ID().String() { // Also fix this comparison
		n.role = "Coordinator"
		n.leader = leaderPeerID
		n.leadershipCount++
		logger.Info("[ELECTION] ROLE ✅ I AM COORDINATOR (term %d)", term)
	} else {
		n.role = "Follower"
		n.leader = leaderPeerID
		n.lastHeartbeat = time.Now()
		n.heartbeatMissed = 0
		logger.Info("[ELECTION] ROLE Following %s (term %d)", leaderID, term)
	}
	n.mutex.Unlock()

	n.electionMutex.Lock()
	n.currentTerm = term
	n.electionMutex.Unlock()
}

// Leader announcement
func (n *Node) announceLeader(leaderID string, term int) {
	announcement := map[string]interface{}{"leader": leaderID, "term": term}
	if err := n.publishMessage(TypeAnnouncement, announcement); err != nil {
		logger.Info("[ERROR] Failed to announce leader: %v", err)
		return
	}

	logger.Info("[ELECTION] Coordinator Announced: %s (term %d)", leaderID, term)

	// Update role
	n.handleAnnouncement(leaderID, term)

	// Start heartbeat if coordinator
	if leaderID == n.host.ID().String() {
		go func() {
			time.Sleep(2 * time.Second)
			n.sendHeartbeats(term)
		}()
	}
}

func (n *Node) sendHeartbeats(term int) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mutex.Lock()
			if n.role != "Coordinator" {
				n.mutex.Unlock()
				return
			}
			n.mutex.Unlock()

			hb := HeartbeatMessage{
				LeaderID: n.host.ID().String(),
				Time:     time.Now().Unix(),
				Term:     term,
			}

			if err := n.publishMessage(TypeHeartbeat, hb); err != nil {
				logger.Info("[ERROR] Heartbeat failed: %v", err)
			} else {
				logger.Info("[ELECTION] HEARTBEAT Sent (term %d)", term)
			}

		case <-n.ctx.Done():
			return
		}
	}
}

func (n *Node) fallbackElection() {
	peers, _ := QueryAllReputations(GlobalReputationDB.ReputationDB)
	if len(peers) == 0 {
		// Use self as fallback
		n.announceLeader(string(n.host.ID()), n.currentTerm+1)
		return
	}

	// Pick highest reputation
	var best NodeReputation
	maxScore := -1.0
	for _, p := range peers {
		if score := calculateReputation(p); score > maxScore {
			maxScore = score
			best = p
		}
	}

	n.announceLeader(best.NodeID, n.currentTerm+1)
}

// Check for leader failure
func (n *Node) CheckLeaderFailure() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		n.mutex.Lock()

		if n.role == "Coordinator" {
			n.mutex.Unlock()
			continue
		}

		if n.lastHeartbeat.IsZero() {
			n.lastHeartbeat = time.Now()
			n.mutex.Unlock()
			continue
		}

		timeSince := time.Since(n.lastHeartbeat)
		if timeSince > heartbeatTimeout {
			n.heartbeatMissed++
			logger.Info("[ELECTION] Missed %d heartbeats (last: %v ago)",
				n.heartbeatMissed, timeSince)

			if n.heartbeatMissed >= heartbeatRetryLimit {
				logger.Error("[ERROR] Leader dead, starting election")
				n.heartbeatMissed = 0
				n.mutex.Unlock()

				if atomic.LoadInt32(&n.isElecting) == 0 {
					go func() {
						peers, _ := QueryAllReputations(GlobalReputationDB.ReputationDB)
						n.StartElection(peers, 0)
					}()
				}
				continue
			}
		} else {
			n.heartbeatMissed = 0
		}
		n.mutex.Unlock()
	}
}

// Reputation publisher
func (n *Node) PeriodicReputationPublisher() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			userCPU, systemCPU, idleCPU, _ := utilities.GetCPUUsage()
			allocMB, totalAllocMB, sysMB := utilities.GetMemoryUsage()
			avgReadMBs, avgWriteMBs, _ := utilities.GetDiskUsage(5)

			actualLatency := utilities.GetActualLatency(n.host)
			actualGeoScore := utilities.GetGeographyScore(n.host)
			actualUptime := utilities.GetActualUptime()

			reputation := NodeReputation{
				NodeID:                n.host.ID().String(),
				Uptime:                actualUptime, // ✅ FIXED
				LeadershipCount:       n.leadershipCount,
				Latency:               actualLatency, // ✅ FIXED
				UserCPU:               userCPU,
				SystemCPU:             systemCPU,
				IdleCPU:               idleCPU,
				MemoryAvailable:       allocMB,
				MemoryAllocationTotal: totalAllocMB,
				MemorySystem:          sysMB,
				AvgReadMBs:            avgReadMBs,
				AvgWriteMBs:           avgWriteMBs,
				GeographyScore:        actualGeoScore, // ✅ FIXED
			}

			UpsertReputation(GlobalReputationDB.ReputationDB, reputation)
			n.publishMessage(TypeReputation, reputation)
			logger.Info("[ELECTION] Reputation Published (score: %.2f)", calculateReputation(reputation))

		case <-n.ctx.Done():
			return
		}
	}
}

// IMPROVED RunFullNode with better mesh waiting
func RunFullNode(ctx context.Context, host host.Host, pubsubObj *pubsub.PubSub, discovery *app.KnowledgeBaseDB) *Node {
	//func RunFullNode(ctx context.Context, host host.Host, pubsubObj *pubsub.PubSub, discovery *app.KnowledgeBaseDB) {
	// Get pre-created topic and subscription from discovery
	var electionTopic *pubsub.Topic
	var electionSub *pubsub.Subscription

	// Check if already created in main.go
	if discovery.ElectionTopic != nil && discovery.ElectionSub != nil {
		electionTopic = discovery.ElectionTopic
		electionSub = discovery.ElectionSub
		logger.Info("[ELECTION] Using pre-created topic and subscription")
	} else {
		// Fallback: create new ones
		logger.Info("[ELECTION] Creating new topic and subscription")
		var err error
		electionTopic, err = pubsubObj.Join("optimusdb")
		if err != nil {
			logger.Error("[ERROR] Cannot join ELECTION topic: %v", err)
			//log.Fatalf("[FATAL] Cannot join election topic: %v", err)
		}

		electionSub, err = electionTopic.Subscribe()
		if err != nil {
			logger.Error("[ERROR] Cannot subscribe ELECTION topic: %v", err)
			//log.Fatalf("[FATAL] Cannot subscribe to election topic: %v", err)
		}
	}

	// Create node with topics already set
	node := NewNode(ctx, host, pubsubObj, discovery)
	node.electionTopic = electionTopic
	node.electionSub = electionSub
	// Add after them:
	//     // Store globally for API access
	electionNodeMutex.Lock()
	GlobalElectionNode = node
	electionNodeMutex.Unlock()
	defer GlobalReputationDB.ReputationDB.Close()

	logger.Info("[ELECTION] Starting OptimusDB Election Node as FOLLOWER")
	node.role = "Follower" // Ensure all start as followers

	// Start listener IMMEDIATELY
	go node.ListenForElectionEvents()
	logger.Info("[ELECTION] ✅ Message listener started")

	// Start background services
	go node.PeriodicReputationPublisher()
	go node.CheckLeaderFailure()
	go node.LogRoleStatus() // For printing the Coordinator / Follower

	// Wait for mesh to form with better logging
	logger.Info("[ELECTION] Waiting for mesh formation...")
	meshCheckInterval := 2 * time.Second
	maxMeshWait := 30 * time.Second
	meshStart := time.Now()

	for time.Since(meshStart) < maxMeshWait {
		discovered := discovery.GetDiscoveredPeers()
		meshPeers := electionTopic.ListPeers()
		allTopics := pubsubObj.GetTopics()

		logger.Info("[ELECTION] Status check:"+
			"- Discovered peers: %d"+
			"  - Mesh peers on 'optimusdb': %d"+
			"  - Subscribed topics: %v", len(discovered), len(meshPeers), allTopics)

		//logger.Info("  - Discovered peers: %d", len(discovered))
		//logger.Info("  - Mesh peers on 'optimusdb': %d", len(meshPeers))
		//logger.Info("  - Subscribed topics: %v", allTopics)

		// Debug: List mesh peers
		if len(meshPeers) > 0 {
			logger.Info("  - Mesh peer IDs:")
			for i, p := range meshPeers {
				logger.Info("    [%d] %s", i+1, p.String())
			}
		}

		// Wait for at least 1 mesh peer (not just discovered)
		if len(meshPeers) >= 1 {
			logger.Info("[ELECTION] ✅ Mesh formed with %d peers!", len(meshPeers))
			break
		}

		if len(discovered) > 0 && len(meshPeers) == 0 {
			logger.Info("[ELECTION] ⚠️ Peers discovered but mesh not formed, waiting...")

			// Try to force mesh formation by publishing a test message
			testMsg := map[string]string{
				"type": "mesh_test",
				"from": string(host.ID()),
				"time": time.Now().Format(time.RFC3339),
			}
			testData, _ := json.Marshal(testMsg)
			if err := electionTopic.Publish(ctx, testData); err != nil {
				logger.Info("[ELECTION] Test publish failed: %v", err)
			} else {
				logger.Info("[ELECTION] Sent test message to stimulate mesh")
			}
		}

		time.Sleep(meshCheckInterval)
	}

	// Give mesh time to stabilize
	logger.Info("[ELECTION] Allowing 5s for mesh stabilization...")
	time.Sleep(5 * time.Second)

	// Final mesh check
	finalMeshPeers := electionTopic.ListPeers()
	logger.Info("[ELECTION] Final mesh status: %d peers in mesh", len(finalMeshPeers))

	// Initialize reputation for self
	selfRep := NodeReputation{
		NodeID:         node.host.ID().String(),
		Uptime:         1.0,
		GeographyScore: 0.5,
	}
	UpsertReputation(GlobalReputationDB.ReputationDB, selfRep)

	// Query all reputations
	peers, err := QueryAllReputations(GlobalReputationDB.ReputationDB)
	if err != nil || len(peers) == 0 {
		peers = []NodeReputation{selfRep}
	}

	// Wait a bit before starting election
	logger.Info("[ELECTION] Waiting 10s before first election...")
	time.Sleep(10 * time.Second)

	logger.Info("[ELECTION] Starting first election with %d candidates", len(peers))
	go node.StartElection(peers, 0)

	// Keep running
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	logger.Info("[ELECTION] Election controller exiting")

	return node
}

func NewNode(ctx context.Context, host host.Host, pubsub *pubsub.PubSub, discovery *app.KnowledgeBaseDB) *Node {
	return &Node{
		ctx:                        ctx,
		host:                       host,
		pubsub:                     pubsub,
		discovery:                  discovery,
		topicManager:               NewTopicManager(pubsub),
		role:                       "Follower",
		votes:                      make(map[string]int),
		votedNodes:                 make(map[string]string),
		votedForInTerm:             make(map[int]string),
		announcedLeaderForElection: make(map[string]string),
		electionPhase:              PhaseIdle,
		currentTerm:                0,
	}
}

// Database functions
func UpsertReputation(db *sql.DB, rep NodeReputation) error {
	query := `INSERT INTO reputation (
		node_id, uptime, leadership_count, latency, user_cpu, system_cpu,
		idle_cpu, memory_available, memory_total_alloc, memory_sys,
		avg_read_mbs, avg_write_mbs, geography_score
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(node_id) DO UPDATE SET
		uptime = excluded.uptime,
		leadership_count = excluded.leadership_count,
		latency = excluded.latency,
		user_cpu = excluded.user_cpu,
		system_cpu = excluded.system_cpu,
		idle_cpu = excluded.idle_cpu,
		memory_available = excluded.memory_available,
		memory_total_alloc = excluded.memory_total_alloc,
		memory_sys = excluded.memory_sys,
		avg_read_mbs = excluded.avg_read_mbs,
		avg_write_mbs = excluded.avg_write_mbs,
		geography_score = excluded.geography_score;`

	_, err := db.Exec(query,
		rep.NodeID, rep.Uptime, rep.LeadershipCount, rep.Latency,
		rep.UserCPU, rep.SystemCPU, rep.IdleCPU,
		rep.MemoryAvailable, rep.MemoryAllocationTotal, rep.MemorySystem,
		rep.AvgReadMBs, rep.AvgWriteMBs, rep.GeographyScore)
	return err
}

func QueryAllReputations(db *sql.DB) ([]NodeReputation, error) {
	rows, err := db.Query(`SELECT * FROM reputation`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reps []NodeReputation
	for rows.Next() {
		var rep NodeReputation
		if err := rows.Scan(
			&rep.NodeID, &rep.Uptime, &rep.LeadershipCount, &rep.Latency,
			&rep.UserCPU, &rep.SystemCPU, &rep.IdleCPU,
			&rep.MemoryAvailable, &rep.MemoryAllocationTotal, &rep.MemorySystem,
			&rep.AvgReadMBs, &rep.AvgWriteMBs, &rep.GeographyScore,
		); err != nil {
			return nil, err
		}
		reps = append(reps, rep)
	}
	return reps, nil
}

func GetReputationByID(db *sql.DB, nodeID string) (NodeReputation, error) {
	row := db.QueryRow(`SELECT * FROM reputation WHERE node_id = ?`, nodeID)
	var rep NodeReputation
	err := row.Scan(
		&rep.NodeID, &rep.Uptime, &rep.LeadershipCount, &rep.Latency,
		&rep.UserCPU, &rep.SystemCPU, &rep.IdleCPU,
		&rep.MemoryAvailable, &rep.MemoryAllocationTotal, &rep.MemorySystem,
		&rep.AvgReadMBs, &rep.AvgWriteMBs, &rep.GeographyScore,
	)
	return rep, err
}

func InsertElectionLog(db *sql.DB, id string, timestamp time.Time, leaderID string, term int, votes map[string]int) error {
	votesJSON, _ := json.Marshal(votes)
	_, err := db.Exec(
		`INSERT INTO election_log (id, timestamp, leader_id, term, votes_json) VALUES (?, ?, ?, ?, ?);`,
		id, timestamp.Format(time.RFC3339), leaderID, term, string(votesJSON))
	return err
}

func (r *ReputationSQLite) SafeExec(query string, args ...interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, err := r.ReputationDB.Exec(query, args...)
	return err
}

// Add this function to periodically log role status
func (n *Node) LogRoleStatus() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mutex.Lock()
			role := n.role
			leader := n.leader
			term := n.currentTerm
			n.mutex.Unlock()

			if role == "Coordinator" {
				logger.Info("[STATUS] 👑 I AM THE COORDINATOR (term %d)", term)
			} else {
				logger.Info("[STATUS] 📋 FOLLOWER following %s (term %d)", leader.String(), term)
			}

		case <-n.ctx.Done():
			return
		}
	}
}

// GetNodeStatus returns the current node's election status
func GetNodeStatus() (role string, leader string, term int, leadershipCount int) {
	electionNodeMutex.RLock()
	defer electionNodeMutex.RUnlock()

	if GlobalElectionNode == nil {
		return "Unknown", "", 0, 0
	}

	GlobalElectionNode.mutex.Lock()
	role = GlobalElectionNode.role
	leader = GlobalElectionNode.leader.String()
	term = GlobalElectionNode.currentTerm
	leadershipCount = GlobalElectionNode.leadershipCount
	GlobalElectionNode.mutex.Unlock()

	return
}

// GetAllPeersReputation retrieves reputation data for all known peers
func GetAllPeersReputation() ([]NodeReputation, error) {
	if GlobalReputationDB == nil || GlobalReputationDB.ReputationDB == nil {
		return nil, fmt.Errorf("reputation database not initialized")
	}

	query := `SELECT node_id, uptime, leadership_count, latency, user_cpu, system_cpu, 
              idle_cpu, memory_available, memory_total_alloc, memory_sys, 
              avg_read_mbs, avg_write_mbs, geography_score 
              FROM reputation ORDER BY node_id`

	rows, err := GlobalReputationDB.ReputationDB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reputations []NodeReputation
	for rows.Next() {
		var rep NodeReputation
		err := rows.Scan(
			&rep.NodeID,
			&rep.Uptime,
			&rep.LeadershipCount,
			&rep.Latency,
			&rep.UserCPU,
			&rep.SystemCPU,
			&rep.IdleCPU,
			&rep.MemoryAvailable,
			&rep.MemoryAllocationTotal,
			&rep.MemorySystem,
			&rep.AvgReadMBs,
			&rep.AvgWriteMBs,
			&rep.GeographyScore,
		)
		if err != nil {
			logger.Error("[ERROR] Failed to scan reputation row: %v", err)
			continue
		}
		reputations = append(reputations, rep)
	}

	return reputations, nil
}

// GetPeerReputation retrieves reputation for a specific peer
func GetPeerReputation(peerID string) (*NodeReputation, error) {
	if GlobalReputationDB == nil || GlobalReputationDB.ReputationDB == nil {
		return nil, fmt.Errorf("reputation database not initialized")
	}

	query := `SELECT node_id, uptime, leadership_count, latency, user_cpu, system_cpu, 
              idle_cpu, memory_available, memory_total_alloc, memory_sys, 
              avg_read_mbs, avg_write_mbs, geography_score 
              FROM reputation WHERE node_id = ?`

	row := GlobalReputationDB.ReputationDB.QueryRow(query, peerID)

	var rep NodeReputation
	err := row.Scan(
		&rep.NodeID,
		&rep.Uptime,
		&rep.LeadershipCount,
		&rep.Latency,
		&rep.UserCPU,
		&rep.SystemCPU,
		&rep.IdleCPU,
		&rep.MemoryAvailable,
		&rep.MemoryAllocationTotal,
		&rep.MemorySystem,
		&rep.AvgReadMBs,
		&rep.AvgWriteMBs,
		&rep.GeographyScore,
	)

	if err == sql.ErrNoRows {
		return nil, nil // Peer not found
	}
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

// CalculateHealthScore calculates health score for a node reputation
func CalculateHealthScore(nr NodeReputation) float64 {
	return calculateReputation(nr)
}

// GetLatestElectionInfo gets the most recent election information
func GetLatestElectionInfo() (leaderID string, term int, timestamp string, err error) {
	if GlobalReputationDB == nil || GlobalReputationDB.ReputationDB == nil {
		return "", 0, "", fmt.Errorf("reputation database not initialized")
	}

	query := `SELECT leader_id, term, timestamp FROM election_log 
              ORDER BY timestamp DESC LIMIT 1`

	row := GlobalReputationDB.ReputationDB.QueryRow(query)
	err = row.Scan(&leaderID, &term, &timestamp)

	if err == sql.ErrNoRows {
		return "", 0, "", nil // No elections yet
	}

	return leaderID, term, timestamp, err
}
