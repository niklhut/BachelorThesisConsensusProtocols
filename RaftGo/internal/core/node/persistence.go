package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/niklhut/raft_go/internal/core/util"
)

type RaftNodePersistence interface {
	// Returns the compaction threshold
	CompactionThreshold() int
	// Saves a snapshot for a node
	SaveSnapshot(snapshot util.Snapshot, forNodeID int) error
	// Loads a snapshot for a node
	LoadSnapshot(forNodeID int) (*util.Snapshot, error)
	// Deletes a snapshot for a node
	DeleteSnapshot(forNodeID int) error
}

// PersistentState represents the durable state persisted to disk across crashes.
type PersistentState struct {
	// Latest term server has seen (initialized to 0)
	CurrentTerm int `json:"currentTerm"`
	// Candidate ID that received vote in current term (or null)
	VotedFor *int `json:"votedFor,omitempty"`
	// Log entries, each containing a command for the state machine
	Log []util.LogEntry `json:"log"`
	// State machine state
	StateMachine map[string]string `json:"stateMachine"`
	// Latest snapshot of the state machine
	Snapshot util.Snapshot `json:"snapshot"`
	// The self peer config
	OwnPeer util.Peer `json:"ownPeer"`
	// List of peers in the cluster
	Peers []util.Peer `json:"peers"`
	// The configuration of the Raft node
	Config util.RaftConfig `json:"config"`

	// Whether the node is currently snapshotting
	IsSnapshotting bool `json:"isSnapshotting"`
	// The persistence of the node
	Persistence RaftNodePersistence `json:"persistence"`
}

type InMemoryRaftNodePersistence struct {
	mu                  sync.RWMutex
	compactionThreshold int
	snapshots           map[int]util.Snapshot
}

func NewInMemoryRaftNodePersistence(compactionThreshold int) *InMemoryRaftNodePersistence {
	return &InMemoryRaftNodePersistence{
		compactionThreshold: compactionThreshold,
		snapshots:           make(map[int]util.Snapshot),
	}
}

func (p *InMemoryRaftNodePersistence) CompactionThreshold() int {
	return p.compactionThreshold
}

func (p *InMemoryRaftNodePersistence) SaveSnapshot(snapshot util.Snapshot, forNodeID int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshots[forNodeID] = snapshot
	return nil
}

func (p *InMemoryRaftNodePersistence) LoadSnapshot(forNodeID int) (*util.Snapshot, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if snapshot, exists := p.snapshots[forNodeID]; exists {
		return &snapshot, nil
	}
	return nil, nil
}

func (p *InMemoryRaftNodePersistence) DeleteSnapshot(forNodeID int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.snapshots, forNodeID)
	return nil
}

type FileRaftNodePersistence struct {
	mu                  sync.Mutex
	compactionThreshold int
	directory           string
}

func NewFileRaftNodePersistence(compactionThreshold int) (*FileRaftNodePersistence, error) {
	dir := filepath.Join(".", "raft-snapshots")

	// Ensure directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	return &FileRaftNodePersistence{
		compactionThreshold: compactionThreshold,
		directory:           dir,
	}, nil
}

func (p *FileRaftNodePersistence) CompactionThreshold() int {
	return p.compactionThreshold
}

func (p *FileRaftNodePersistence) filePath(forNodeID int) string {
	return filepath.Join(p.directory, fmt.Sprintf("snapshot_%d.json", forNodeID))
}

func (p *FileRaftNodePersistence) SaveSnapshot(snapshot util.Snapshot, forNodeID int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	path := p.filePath(forNodeID)
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	return nil
}

func (p *FileRaftNodePersistence) LoadSnapshot(forNodeID int) (*util.Snapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	path := p.filePath(forNodeID)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot: %w", err)
	}

	var snapshot util.Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	return &snapshot, nil
}

func (p *FileRaftNodePersistence) DeleteSnapshot(forNodeID int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	path := p.filePath(forNodeID)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}

	if err := os.Remove(path); err != nil {
		return fmt.Errorf("failed to delete snapshot: %w", err)
	}

	return nil
}
