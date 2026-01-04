package election

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"optimusdb/app"
	"optimusdb/config"
	"optimusdb/logger"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	_ "github.com/mattn/go-sqlite3"
)

/*
================================================================================
RAFT-BASED LEADER ELECTION FOR OPTIMUSDB
================================================================================

This implementation follows the Raft consensus algorithm for leader election.
Key principles:

1. **Election Safety**: At most one leader per term
2. **Leader Completeness**: Once elected, leader serves until failure
3. **Term Monotonicity**: Terms only increase, never decrease
4. **Randomized Timeouts**: Prevents split votes

DIFFERENCES FROM ORIGINAL:
- ❌ Removed: Election IDs (term IS the election identifier)
- ❌ Removed: Reputation-based voting (simplified)
- ❌ Removed: Fallback elections (unsafe)
- ❌ Removed: Initiator selection (any node can start)
- ✅ Added: Persistent votedFor (prevents double voting)
- ✅ Added: Randomized timeouts (prevents split votes)
- ✅ Added: Proper quorum enforcement
- ✅ Kept: FloodPublish for message delivery
- ✅ Kept: Heartbeat monitoring
- ✅ Kept: API compatibility

ROLES:
- Follower: Normal state, follows heartbeats
- Candidate: Actively running for election
- Coordinator: Elected leader (renamed from "Leader" to match existing API)

================================================================================
*/

const (
	// Timing constants (from Raft paper)
	heartbeatInterval  = 5 * time.Second        // Leader sends heartbeats
	electionTimeoutMin = 150 * time.Millisecond // Min election timeout
	electionTimeoutMax = 300 * time.Millisecond // Max election timeout

	// Message types
	TypeVoteRequest  = "vote_request"
	TypeVoteReply    = "vote_reply"
	TypeHeartbeat    = "heartbeat"
	TypeAnnouncement = "announcement"
)

// Raft roles
const (
	RoleFollower    = "Follower"
	RoleCandidate   = "Candidate"
	RoleCoordinator = "Coordinator" // Matches existing API terminology
)

// Message types
type VoteRequest struct {
	Term        int    `json:"term"`
	CandidateID string `json:"candidate_id"`
}

type VoteReply struct {
	Term        int    `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	VoterID     string `json:"voter_id"`
}

type HeartbeatMessage struct {
	Term     int    `json:"term"`
	LeaderID string `json:"leader_id"`
	Time     int64  `json:"time"`
}

type CoreMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// RaftNode is the main election node structure
type RaftNode struct {
	ctx  context.Context
	host host.Host

	// GossipSub communication
	pubsub       *pubsub.PubSub
	topic        *pubsub.Topic
	subscription *pubsub.Subscription

	// Raft persistent state
	persistentState *RaftPersistentState

	// Raft volatile state
	role            string
	leader          peer.ID
	votesReceived   map[string]bool
	electionTimeout time.Duration
	electionTimer   *time.Timer
	lastHeartbeat   time.Time

	// Cluster info
	clusterSize     int
	quorumSize      int
	leadershipCount int

	// Discovery integration
	discovery *app.KnowledgeBaseDB

	// Thread safety
	mu sync.Mutex

	// Atomic flags
	isElecting int32

	// Shutdown
	shutdown chan struct{}
}

// Global election node for API access
var (
	GlobalElectionNode *RaftNode
	globalNodeMutex    sync.RWMutex
)

/*
================================================================================
NODE INITIALIZATION
================================================================================
*/

// RunFullNode initializes and starts the Raft election system
// This maintains the same signature as the original for compatibility
func RunFullNode(ctx context.Context, host host.Host, pubsubObj *pubsub.PubSub, discovery *app.KnowledgeBaseDB) *RaftNode {
	logger.Election("[RAFT] ════════════════════════════════════════")
	logger.Election("[RAFT] Initializing Raft-Based Election")
	logger.Election("[RAFT] ════════════════════════════════════════")

	// Determine data directory for persistent state
	dataDir := filepath.Join(
		os.Getenv("HOME"),
		".cache",
		"optimusdb",
		*config.FlagRepo,
		"optimusdb",
		"raft",
	)

	// Use existing topic/subscription from discovery if available
	var electionTopic *pubsub.Topic
	var electionSub *pubsub.Subscription

	if discovery.ElectionTopic != nil && discovery.ElectionSub != nil {
		electionTopic = discovery.ElectionTopic
		electionSub = discovery.ElectionSub
		logger.Election("[RAFT] Using pre-created GossipSub topic")
	} else {
		var err error
		electionTopic, err = pubsubObj.Join("optimusdb")
		if err != nil {
			logger.Error("[RAFT] FATAL: Cannot join topic: %v", err)
			return nil
		}

		electionSub, err = electionTopic.Subscribe()
		if err != nil {
			logger.Error("[RAFT] FATAL: Cannot subscribe: %v", err)
			return nil
		}
		logger.Election("[RAFT] Created new GossipSub topic")
	}

	// Get cluster size
	discoveredPeers := discovery.GetDiscoveredPeers()
	clusterSize := len(discoveredPeers) + 1 // +1 for self

	if clusterSize < 1 {
		clusterSize = 1
	}

	logger.Election("[RAFT] Cluster size: %d nodes", clusterSize)
	logger.Election("[RAFT] Quorum required: %d votes", (clusterSize/2)+1)

	// Create Raft node
	node, err := NewRaftNode(ctx, host, pubsubObj, electionTopic, electionSub, dataDir, clusterSize, discovery)
	if err != nil {
		logger.Error("[RAFT] FATAL: Failed to create node: %v", err)
		return nil
	}

	// Store globally for API access
	globalNodeMutex.Lock()
	GlobalElectionNode = node
	globalNodeMutex.Unlock()

	logger.Election("[RAFT] Node ID: %s", host.ID().String())
	logger.Election("[RAFT] Initial role: %s", RoleFollower)

	// Start node
	node.Start()

	logger.Election("[RAFT] ════════════════════════════════════════")
	logger.Election("[RAFT] Raft Election System Running")
	logger.Election("[RAFT] ════════════════════════════════════════")

	return node
}

// NewRaftNode creates a new Raft node instance
func NewRaftNode(
	ctx context.Context,
	h host.Host,
	ps *pubsub.PubSub,
	topic *pubsub.Topic,
	sub *pubsub.Subscription,
	dataDir string,
	clusterSize int,
	discovery *app.KnowledgeBaseDB,
) (*RaftNode, error) {

	// Initialize persistent state
	persistentState, err := NewRaftPersistentState(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to init persistent state: %w", err)
	}

	node := &RaftNode{
		ctx:             ctx,
		host:            h,
		pubsub:          ps,
		topic:           topic,
		subscription:    sub,
		persistentState: persistentState,
		role:            RoleFollower,
		votesReceived:   make(map[string]bool),
		clusterSize:     clusterSize,
		quorumSize:      (clusterSize / 2) + 1,
		discovery:       discovery,
		shutdown:        make(chan struct{}),
	}

	return node, nil
}

// Start begins the Raft protocol
func (rn *RaftNode) Start() {
	logger.Election("[RAFT] Starting Raft protocol")

	// Reset election timeout
	rn.resetElectionTimeout()

	// Start background goroutines
	go rn.listenForMessages()
	go rn.monitorElectionTimeout()
	go rn.logStatus()

	logger.Election("[RAFT] Background services started")
}

/*
================================================================================
ELECTION TIMEOUT HANDLING
================================================================================
*/

// resetElectionTimeout sets a new random timeout
func (rn *RaftNode) resetElectionTimeout() {
	// Random timeout between 150-300ms
	randomMs := rand.Intn(int(electionTimeoutMax - electionTimeoutMin))
	rn.electionTimeout = electionTimeoutMin + time.Duration(randomMs)

	rn.lastHeartbeat = time.Now()

	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}

	rn.electionTimer = time.AfterFunc(rn.electionTimeout, func() {
		rn.onElectionTimeout()
	})
}

// monitorElectionTimeout is a backup monitor
func (rn *RaftNode) monitorElectionTimeout() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rn.mu.Lock()
			role := rn.role
			timeSince := time.Since(rn.lastHeartbeat)
			rn.mu.Unlock()

			if role == RoleCoordinator {
				continue
			}

			if timeSince > 15*time.Second {
				logger.Warn("[RAFT] Heartbeat timeout: %v since last heartbeat", timeSince)
				go rn.onElectionTimeout()
			}

		case <-rn.shutdown:
			return
		}
	}
}

// onElectionTimeout is called when election timeout expires
func (rn *RaftNode) onElectionTimeout() {
	rn.mu.Lock()
	role := rn.role
	rn.mu.Unlock()

	if role == RoleCoordinator {
		return // Leaders don't timeout
	}

	logger.Election("[RAFT] Election timeout expired, starting election")
	rn.startElection()
}

/*
================================================================================
ELECTION LOGIC
================================================================================
*/

// startElection transitions to Candidate and begins election
func (rn *RaftNode) startElection() {
	// Prevent concurrent elections
	if !atomic.CompareAndSwapInt32(&rn.isElecting, 0, 1) {
		logger.Election("[RAFT] Election already in progress")
		return
	}
	defer atomic.StoreInt32(&rn.isElecting, 0)

	// Become candidate
	rn.mu.Lock()
	rn.role = RoleCandidate
	rn.votesReceived = make(map[string]bool)
	rn.mu.Unlock()

	// Increment term and vote for self
	newTerm, err := rn.persistentState.IncrementTerm()
	if err != nil {
		logger.Error("[RAFT] Failed to increment term: %v", err)
		return
	}

	if err := rn.persistentState.VoteFor(rn.host.ID().String()); err != nil {
		logger.Error("[RAFT] Failed to vote for self: %v", err)
		return
	}

	// Count self vote
	rn.mu.Lock()
	rn.votesReceived[rn.host.ID().String()] = true
	voteCount := len(rn.votesReceived)
	rn.mu.Unlock()

	logger.Election("[RAFT] Started election for term %d (vote %d/%d)", newTerm, voteCount, rn.quorumSize)

	// Check if already won (single node cluster)
	if voteCount >= rn.quorumSize {
		rn.becomeLeader(newTerm)
		return
	}

	// Request votes from peers
	voteReq := VoteRequest{
		Term:        newTerm,
		CandidateID: rn.host.ID().String(),
	}

	if err := rn.publishMessage(TypeVoteRequest, voteReq); err != nil {
		logger.Error("[RAFT] Failed to send vote requests: %v", err)
	}

	// Reset election timeout
	rn.resetElectionTimeout()
}

// handleVoteRequest processes vote requests from candidates
func (rn *RaftNode) handleVoteRequest(req VoteRequest) {
	currentTerm, votedFor := rn.persistentState.GetState()

	// Reject if stale term
	if req.Term < currentTerm {
		logger.Election("[RAFT] Rejecting stale vote request (term %d < %d)", req.Term, currentTerm)
		rn.sendVoteReply(req.CandidateID, currentTerm, false)
		return
	}

	// Update to higher term
	if req.Term > currentTerm {
		logger.Election("[RAFT] Updating to term %d", req.Term)
		if err := rn.persistentState.UpdateTerm(req.Term); err != nil {
			logger.Error("[RAFT] Failed to update term: %v", err)
			return
		}

		rn.mu.Lock()
		rn.role = RoleFollower
		rn.mu.Unlock()

		currentTerm = req.Term
		votedFor = ""
	}

	// Grant vote if we can
	if rn.persistentState.CanVoteFor(req.CandidateID) {
		if err := rn.persistentState.VoteFor(req.CandidateID); err != nil {
			logger.Error("[RAFT] Failed to record vote: %v", err)
			return
		}

		logger.Election("[RAFT] ✅ Granted vote to %s for term %d", req.CandidateID, currentTerm)
		rn.resetElectionTimeout()
		rn.sendVoteReply(req.CandidateID, currentTerm, true)
	} else {
		logger.Election("[RAFT] ❌ Already voted for %s in term %d", votedFor, currentTerm)
		rn.sendVoteReply(req.CandidateID, currentTerm, false)
	}
}

// handleVoteReply processes vote replies
func (rn *RaftNode) handleVoteReply(reply VoteReply) {
	currentTerm, _ := rn.persistentState.GetState()

	rn.mu.Lock()

	// Ignore if not candidate
	if rn.role != RoleCandidate {
		rn.mu.Unlock()
		return
	}

	// Ignore stale replies
	if reply.Term < currentTerm {
		rn.mu.Unlock()
		return
	}

	// Step down if higher term
	if reply.Term > currentTerm {
		rn.mu.Unlock()
		logger.Election("[RAFT] Discovered higher term %d, stepping down", reply.Term)
		rn.persistentState.UpdateTerm(reply.Term)
		rn.mu.Lock()
		rn.role = RoleFollower
		rn.mu.Unlock()
		rn.resetElectionTimeout()
		return
	}

	// Count vote
	if reply.VoteGranted {
		rn.votesReceived[reply.VoterID] = true
		voteCount := len(rn.votesReceived)

		logger.Election("[RAFT] Received vote from %s (total: %d/%d)", reply.VoterID, voteCount, rn.quorumSize)

		// Check if won
		if voteCount >= rn.quorumSize {
			logger.Election("[RAFT] 🎉 WON election with %d/%d votes", voteCount, rn.quorumSize)
			rn.role = RoleCoordinator
			rn.mu.Unlock()
			rn.becomeLeader(currentTerm)
			return
		}
	}

	rn.mu.Unlock()
}

// sendVoteReply sends a vote reply
func (rn *RaftNode) sendVoteReply(candidateID string, term int, granted bool) {
	reply := VoteReply{
		Term:        term,
		VoteGranted: granted,
		VoterID:     rn.host.ID().String(),
	}

	if err := rn.publishMessage(TypeVoteReply, reply); err != nil {
		logger.Error("[RAFT] Failed to send vote reply: %v", err)
	}
}

/*
================================================================================
LEADER OPERATIONS
================================================================================
*/

// becomeLeader transitions to leader role
func (rn *RaftNode) becomeLeader(term int) {
	logger.Election("[RAFT] 👑 Became COORDINATOR for term %d", term)

	rn.mu.Lock()
	rn.role = RoleCoordinator
	rn.leadershipCount++
	rn.mu.Unlock()

	// Announce leadership
	rn.announceLeader(rn.host.ID().String(), term)

	// Start heartbeats
	go rn.sendPeriodicHeartbeats()
}

// sendPeriodicHeartbeats sends heartbeats while leader
func (rn *RaftNode) sendPeriodicHeartbeats() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rn.mu.Lock()
			role := rn.role
			rn.mu.Unlock()

			if role != RoleCoordinator {
				logger.Election("[RAFT] No longer coordinator, stopping heartbeats")
				return
			}

			rn.sendHeartbeat()

		case <-rn.shutdown:
			return
		}
	}
}

// sendHeartbeat sends a single heartbeat
func (rn *RaftNode) sendHeartbeat() {
	currentTerm, _ := rn.persistentState.GetState()

	hb := HeartbeatMessage{
		Term:     currentTerm,
		LeaderID: rn.host.ID().String(),
		Time:     time.Now().Unix(),
	}

	if err := rn.publishMessage(TypeHeartbeat, hb); err != nil {
		logger.Error("[RAFT] Heartbeat failed: %v", err)
	} else {
		logger.Info("[RAFT] 💓 Heartbeat sent (term %d)", currentTerm)
	}
}

// handleHeartbeat processes heartbeats from leader
func (rn *RaftNode) handleHeartbeat(hb HeartbeatMessage) {
	currentTerm, _ := rn.persistentState.GetState()

	// Update last heartbeat time
	rn.lastHeartbeat = time.Now()

	// Ignore stale heartbeats
	if hb.Term < currentTerm {
		return
	}

	// Update to higher term if needed
	if hb.Term > currentTerm {
		logger.Election("[RAFT] Updating to term %d from heartbeat", hb.Term)
		rn.persistentState.UpdateTerm(hb.Term)
		currentTerm = hb.Term
	}

	// If we're coordinator and receive heartbeat from another coordinator
	rn.mu.Lock()
	if rn.role == RoleCoordinator && hb.LeaderID != rn.host.ID().String() {
		logger.Warn("[RAFT] Detected competing coordinator, stepping down")
		rn.role = RoleFollower
	}

	// Update leader
	leaderID, err := peer.Decode(hb.LeaderID)
	if err != nil {
		rn.mu.Unlock()
		return
	}

	if rn.role != RoleCoordinator {
		rn.role = RoleFollower
		rn.leader = leaderID
	}
	rn.mu.Unlock()

	// Reset election timeout
	rn.resetElectionTimeout()
}

// announceLeader broadcasts leader announcement
func (rn *RaftNode) announceLeader(leaderID string, term int) {
	announcement := map[string]interface{}{
		"leader": leaderID,
		"term":   term,
	}

	if err := rn.publishMessage(TypeAnnouncement, announcement); err != nil {
		logger.Error("[RAFT] Failed to announce leader: %v", err)
	} else {
		logger.Election("[RAFT] 📢 Announced coordinator: %s (term %d)", leaderID, term)
	}
}

/*
================================================================================
MESSAGE HANDLING
================================================================================
*/

// publishMessage sends a message via GossipSub
func (rn *RaftNode) publishMessage(msgType string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload failed: %w", err)
	}

	core := CoreMessage{Type: msgType, Payload: data}
	coreData, err := json.Marshal(core)
	if err != nil {
		return fmt.Errorf("marshal core failed: %w", err)
	}

	if err := rn.topic.Publish(rn.ctx, coreData); err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}

	return nil
}

// listenForMessages receives messages from GossipSub
func (rn *RaftNode) listenForMessages() {
	logger.Election("[RAFT] Starting message listener")

	for {
		msg, err := rn.subscription.Next(rn.ctx)
		if err != nil {
			if rn.ctx.Err() != nil {
				return
			}
			logger.Error("[RAFT] Failed to receive message: %v", err)
			continue
		}

		// Ignore own messages
		if msg.ReceivedFrom == rn.host.ID() {
			continue
		}

		// Deserialize
		var core CoreMessage
		if err := json.Unmarshal(msg.Data, &core); err != nil {
			logger.Error("[RAFT] Failed to unmarshal: %v", err)
			continue
		}

		// Route to handler
		rn.handleMessage(core)
	}
}

// handleMessage routes messages to appropriate handlers
func (rn *RaftNode) handleMessage(core CoreMessage) {
	switch core.Type {
	case TypeVoteRequest:
		var req VoteRequest
		if err := json.Unmarshal(core.Payload, &req); err != nil {
			logger.Error("[RAFT] Failed to unmarshal vote request: %v", err)
			return
		}
		rn.handleVoteRequest(req)

	case TypeVoteReply:
		var reply VoteReply
		if err := json.Unmarshal(core.Payload, &reply); err != nil {
			logger.Error("[RAFT] Failed to unmarshal vote reply: %v", err)
			return
		}
		rn.handleVoteReply(reply)

	case TypeHeartbeat:
		var hb HeartbeatMessage
		if err := json.Unmarshal(core.Payload, &hb); err != nil {
			logger.Error("[RAFT] Failed to unmarshal heartbeat: %v", err)
			return
		}
		rn.handleHeartbeat(hb)
	}
}

/*
================================================================================
STATUS AND MONITORING
================================================================================
*/

// logStatus periodically logs node status
func (rn *RaftNode) logStatus() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rn.mu.Lock()
			role := rn.role
			leader := rn.leader
			term, _ := rn.persistentState.GetState()
			rn.mu.Unlock()

			if role == RoleCoordinator {
				logger.Info("[STATUS] 👑 COORDINATOR (term %d)", term)
			} else {
				logger.Info("[STATUS] 📋 FOLLOWER following %s (term %d)", leader.String(), term)
			}

		case <-rn.shutdown:
			return
		}
	}
}

// GetNodeStatus returns current status for API (maintains compatibility)
func GetNodeStatus() (role string, leader string, term int, leadershipCount int) {
	globalNodeMutex.RLock()
	defer globalNodeMutex.RUnlock()

	if GlobalElectionNode == nil {
		return "Unknown", "", 0, 0
	}

	GlobalElectionNode.mu.Lock()
	role = GlobalElectionNode.role
	leader = GlobalElectionNode.leader.String()
	term, _ = GlobalElectionNode.persistentState.GetState()
	leadershipCount = GlobalElectionNode.leadershipCount
	GlobalElectionNode.mu.Unlock()

	return
}

// Additional compatibility functions for existing API
func GetLatestElectionInfo() (leaderID string, term int, timestamp string, err error) {
	globalNodeMutex.RLock()
	defer globalNodeMutex.RUnlock()

	if GlobalElectionNode == nil {
		return "", 0, "", nil
	}

	GlobalElectionNode.mu.Lock()
	leaderID = GlobalElectionNode.leader.String()
	term, _ = GlobalElectionNode.persistentState.GetState()
	GlobalElectionNode.mu.Unlock()

	return leaderID, term, time.Now().Format(time.RFC3339), nil
}

// Dummy types for API compatibility (reputation system removed)
type NodeReputation struct {
	NodeID string `json:"nodeId"`
}

func GetAllPeersReputation() ([]NodeReputation, error) {
	return []NodeReputation{}, nil
}

func GetPeerReputation(peerID string) (*NodeReputation, error) {
	return &NodeReputation{NodeID: peerID}, nil
}

func CalculateHealthScore(nr NodeReputation) float64 {
	return 100.0
}

/*
================================================================================
REPUTATION DB COMPATIBILITY LAYER
================================================================================

The old election system used ReputationDB to store node metrics and calculate
reputation scores. The new Raft system doesn't need this, but we keep these
functions for backward compatibility to avoid breaking main.go and other code
that references GlobalReputationDB.

These are now no-op stubs that maintain the API contract.
================================================================================
*/

// ReputationSQLite wraps the SQLite database (compatibility stub)
type ReputationSQLite struct {
	ReputationDB *sql.DB
	mu           sync.Mutex
}

// Global reputation DB for compatibility
var GlobalReputationDB *ReputationSQLite

// InitReputationDB initializes the reputation database (compatibility stub)
// This is called from main.go but is now a minimal no-op implementation
func InitReputationDB() (*ReputationSQLite, error) {
	logger.Election("[RAFT-COMPAT] ReputationDB compatibility layer initialized (no-op)")
	logger.Election("[RAFT-COMPAT] Raft doesn't use reputation-based voting")

	// Create minimal stub database for compatibility
	rdbmsCache := filepath.Join(
		filepath.Join(
			filepath.Join(os.Getenv("HOME"), ".cache"),
			"optimusdb",
			*config.FlagRepo,
			"optimusdb",
		),
		"optimusreputation.db",
	)

	// Ensure directory exists
	dir := filepath.Dir(rdbmsCache)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for Reputation DB: %w", err)
	}

	// Open database (for compatibility with any code that queries it)
	db, err := sql.Open("sqlite3", rdbmsCache)
	if err != nil {
		logger.Error("[RAFT-COMPAT] Cannot open SQLite DB for Reputation: %v", err)
		return nil, err
	}

	GlobalReputationDB = &ReputationSQLite{ReputationDB: db}

	// Create minimal schema (for compatibility)
	if err := GlobalReputationDB.createReputationDB(); err != nil {
		logger.Warn("[RAFT-COMPAT] Table creation warning (non-critical): %v", err)
	}

	logger.Election("[RAFT-COMPAT] Reputation DB stub ready at: %s", rdbmsCache)
	return GlobalReputationDB, nil
}

// createReputationDB creates minimal tables (compatibility stub)
func (rep *ReputationSQLite) createReputationDB() error {
	// Minimal table for compatibility
	tableQuery := `CREATE TABLE IF NOT EXISTS reputation (
		node_id TEXT PRIMARY KEY,
		uptime REAL DEFAULT 1.0,
		leadership_count INTEGER DEFAULT 0
	);`

	if _, err := rep.ReputationDB.Exec(tableQuery); err != nil {
		return err
	}

	return nil
}

// SafeExec executes a database query with mutex protection (compatibility stub)
func (r *ReputationSQLite) SafeExec(query string, args ...interface{}) error {
	if r == nil || r.ReputationDB == nil {
		return nil // No-op if not initialized
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	_, err := r.ReputationDB.Exec(query, args...)
	return err
}
