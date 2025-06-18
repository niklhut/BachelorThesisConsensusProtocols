package util

import (
	"time"
)

// LeaderState represents the leader-only volatile state tracking next log entry to send to each follower.
type LeaderState struct {
	// For each follower, index of the next log entry to send to that follower
	NextIndex map[int]int
	// For each follower, index of highest log entry known to be replicated on that follower
	MatchIndex map[int]int
}

// LogEntry is a single entry in the replicated log.
type LogEntry struct {
	// Term when the entry was received by the leader
	Term int `json:"term"`
	// Key for the key-value operation, optional for no-op entries
	Key *string `json:"key,omitempty"`
	// Optional value (null if this is a deletion or no-op)
	Value *string `json:"value,omitempty"`
}

// Peer represents a peer in the cluster.
type Peer struct {
	// The ID of the peer
	ID int `json:"id"`
	// The address of the peer
	Address string `json:"address"`
	// The port of the peer
	Port int `json:"port"`
}

// RaftConfig represents the configuration for the Raft node.
type RaftConfig struct {
	// The range of election timeout in milliseconds
	ElectionTimeoutMinMs int `json:"electionTimeoutMinMs"`
	ElectionTimeoutMaxMs int `json:"electionTimeoutMaxMs"`

	// The interval of heartbeats in milliseconds
	HeartbeatIntervalMs int `json:"heartbeatIntervalMs"`

	// The threshold of log entries to compact
	CompactionThreshold int `json:"compactionThreshold"`
}

// NewRaftConfig creates a new default RaftConfig.
func NewRaftConfig() RaftConfig {
	return RaftConfig{
		ElectionTimeoutMinMs: 300,
		ElectionTimeoutMaxMs: 600,
		HeartbeatIntervalMs:  100,
		CompactionThreshold:  1000,
	}
}

// ServerState represents the current role of the server in the Raft cluster.
type ServerState string

const (
	ServerStateFollower  ServerState = "follower"
	ServerStateCandidate ServerState = "candidate"
	ServerStateLeader    ServerState = "leader"
)

// Snapshot represents a snapshot of the entire state machine.
type Snapshot struct {
	// Index of last log entry included in the snapshot
	LastIncludedIndex int `json:"lastIncludedIndex"`
	// Term of last log entry included in the snapshot
	LastIncludedTerm int `json:"lastIncludedTerm"`
	// State machine state
	StateMachine map[string]string `json:"stateMachine"`
}

// VolatileState represents the volatile state maintained in memory.
type VolatileState struct {
	// Index of highest log entry known to be committed
	CommitIndex int `json:"commitIndex"`
	// Index of highest log entry applied to state machine
	LastApplied int `json:"lastApplied"`
	// Current role/state of this server
	State ServerState `json:"state"`
	// ID of the current leader, if this node is a follower
	CurrentLeaderID *int `json:"currentLeaderID,omitempty"`
	// Last heartbeat time
	LastHeartbeat time.Time `json:"lastHeartbeat"`
	// Election timeout
	ElectionTimeout int `json:"electionTimeout"`
}

type InstallSnapshotRequest struct {
	// Term of the leader
	Term int `json:"term"`
	// ID of the leader
	LeaderID int `json:"leaderID"`
	// Snapshot to install
	Snapshot Snapshot `json:"snapshot"`
}

type InstallSnapshotResponse struct {
	// Term of the follower
	Term int `json:"term"`
}
