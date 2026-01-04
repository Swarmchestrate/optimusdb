package election

import (
	"encoding/json"
	"fmt"
	"optimusdb/logger"
	"os"
	"path/filepath"
	"sync"
)

/*
================================================================================
RAFT PERSISTENT STATE - CRITICAL FOR ELECTION SAFETY
================================================================================

This file implements persistent storage for Raft consensus state variables.
The state MUST survive process restarts to prevent double-voting and maintain
term monotonicity.

RAFT PERSISTENT STATE (from Raft paper):
1. currentTerm: Latest term server has seen (increases monotonically)
2. votedFor: CandidateId that received vote in current term (or "")

SAFETY REQUIREMENT:
These values MUST be written to stable storage BEFORE responding to RPCs.
Failure to persist can cause:
- Split-brain (multiple leaders in same term)
- Double voting (voting twice in same term after restart)
- Term number regression

IMPLEMENTATION:
- Atomic writes using rename
- JSON format for debugging/inspection
- Thread-safe with mutex
- fsync for durability
================================================================================
*/

// RaftPersistentState holds term and vote information
type RaftPersistentState struct {
	CurrentTerm int    `json:"current_term"` // Latest term seen
	VotedFor    string `json:"voted_for"`    // Candidate voted for in current term

	mu       sync.Mutex
	filePath string
}

// NewRaftPersistentState creates or loads persistent state from disk
func NewRaftPersistentState(dataDir string) (*RaftPersistentState, error) {
	// Ensure directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	filePath := filepath.Join(dataDir, "raft_state.json")

	rps := &RaftPersistentState{
		CurrentTerm: 0,
		VotedFor:    "",
		filePath:    filePath,
	}

	// Try to load existing state
	if err := rps.load(); err != nil {
		if os.IsNotExist(err) {
			// First run - initialize with defaults
			logger.Election("[RAFT-STATE] Initializing new persistent state at %s", filePath)
			if err := rps.save(); err != nil {
				return nil, fmt.Errorf("failed to save initial state: %w", err)
			}
		} else {
			// Corrupted state file - reinitialize
			logger.Warn("[RAFT-STATE] Corrupted state file, reinitializing: %v", err)
			if err := rps.save(); err != nil {
				return nil, fmt.Errorf("failed to save repaired state: %w", err)
			}
		}
	} else {
		logger.Election("[RAFT-STATE] Loaded: term=%d, votedFor=%s", rps.CurrentTerm, rps.VotedFor)
	}

	return rps, nil
}

// save writes state to disk atomically
func (rps *RaftPersistentState) save() error {
	rps.mu.Lock()
	defer rps.mu.Unlock()

	// Serialize
	data, err := json.MarshalIndent(rps, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	// Atomic write: write temp, sync, rename
	tmpPath := rps.filePath + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("write temp file failed: %w", err)
	}

	// Sync to ensure durability
	f, err := os.OpenFile(tmpPath, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open for sync failed: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync failed: %w", err)
	}
	f.Close()

	// Atomic rename
	if err := os.Rename(tmpPath, rps.filePath); err != nil {
		return fmt.Errorf("rename failed: %w", err)
	}

	return nil
}

// load reads state from disk
func (rps *RaftPersistentState) load() error {
	data, err := os.ReadFile(rps.filePath)
	if err != nil {
		return err
	}

	rps.mu.Lock()
	defer rps.mu.Unlock()

	return json.Unmarshal(data, rps)
}

// GetState returns current term and votedFor (thread-safe)
func (rps *RaftPersistentState) GetState() (term int, votedFor string) {
	rps.mu.Lock()
	defer rps.mu.Unlock()
	return rps.CurrentTerm, rps.VotedFor
}

// SetTermAndVote atomically updates term and votedFor, then persists
// CRITICAL: Must be called BEFORE responding to vote requests
func (rps *RaftPersistentState) SetTermAndVote(term int, candidateID string) error {
	rps.mu.Lock()
	rps.CurrentTerm = term
	rps.VotedFor = candidateID
	rps.mu.Unlock()

	// CRITICAL: Persist before returning
	if err := rps.save(); err != nil {
		return fmt.Errorf("failed to persist: %w", err)
	}

	logger.Election("[RAFT-STATE] Persisted: term=%d, votedFor=%s", term, candidateID)
	return nil
}

// IncrementTerm advances to next term and clears vote
func (rps *RaftPersistentState) IncrementTerm() (int, error) {
	rps.mu.Lock()
	rps.CurrentTerm++
	rps.VotedFor = ""
	newTerm := rps.CurrentTerm
	rps.mu.Unlock()

	if err := rps.save(); err != nil {
		return 0, fmt.Errorf("failed to persist term increment: %w", err)
	}

	logger.Election("[RAFT-STATE] Advanced to term %d", newTerm)
	return newTerm, nil
}

// UpdateTerm updates to higher term and clears vote
func (rps *RaftPersistentState) UpdateTerm(newTerm int) error {
	rps.mu.Lock()

	if newTerm <= rps.CurrentTerm {
		rps.mu.Unlock()
		return fmt.Errorf("new term %d not higher than current %d", newTerm, rps.CurrentTerm)
	}

	rps.CurrentTerm = newTerm
	rps.VotedFor = ""
	rps.mu.Unlock()

	if err := rps.save(); err != nil {
		return fmt.Errorf("failed to persist term update: %w", err)
	}

	logger.Election("[RAFT-STATE] Updated to term %d", newTerm)
	return nil
}

// VoteFor records a vote for a candidate in current term
func (rps *RaftPersistentState) VoteFor(candidateID string) error {
	rps.mu.Lock()

	currentVote := rps.VotedFor

	if currentVote != "" && currentVote != candidateID {
		rps.mu.Unlock()
		return fmt.Errorf("already voted for %s", currentVote)
	}

	rps.VotedFor = candidateID
	term := rps.CurrentTerm
	rps.mu.Unlock()

	if err := rps.save(); err != nil {
		return fmt.Errorf("failed to persist vote: %w", err)
	}

	logger.Election("[RAFT-STATE] Voted for %s in term %d", candidateID, term)
	return nil
}

// CanVoteFor checks if we can vote for a candidate
func (rps *RaftPersistentState) CanVoteFor(candidateID string) bool {
	rps.mu.Lock()
	defer rps.mu.Unlock()

	return rps.VotedFor == "" || rps.VotedFor == candidateID
}
