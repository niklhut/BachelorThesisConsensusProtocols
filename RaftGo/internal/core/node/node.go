// internal/core/node/node.go

package node

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/niklhut/raft_go/internal/core/util"
)

// RaftPeerTransport defines the interface for peer-to-peer communication.
type RaftPeerTransport interface {
	AppendEntries(
		ctx context.Context,
		request util.AppendEntriesRequest,
		toPeer util.Peer,
	) (util.AppendEntriesResponse, error)

	RequestVote(
		ctx context.Context,
		request util.RequestVoteRequest,
		toPeer util.Peer,
	) (util.RequestVoteResponse, error)

	InstallSnapshot(
		ctx context.Context,
		request util.InstallSnapshotRequest,
		toPeer util.Peer,
	) (util.InstallSnapshotResponse, error)
}

// RaftNode represents a single node in the Raft cluster.
// All access to its internal state (PersistentState, VolatileState, LeaderState)
// must be synchronized using the mutex.
type RaftNode struct {
	mu sync.RWMutex // Mutex to protect the node's state

	transport RaftPeerTransport // An interface for peer communication
	logger    *slog.Logger      // Structured logger

	heartbeatCancel context.CancelFunc
	heartbeatMu     sync.Mutex
	wg              sync.WaitGroup

	// Raft states (protected by mu)
	persistentState PersistentState
	volatileState   util.VolatileState
	leaderState     util.LeaderState
}

// Majority returns the number of votes needed to become leader or commit a log entry.
func (rn *RaftNode) Majority() int {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return (len(rn.persistentState.Peers)+1)/2 + 1
}

// NewRaftNode creates and initializes a new RaftNode.
// This is the Go constructor function.
func NewRaftNode(ownPeer util.Peer, peers []util.Peer, config util.RaftConfig, transport RaftPeerTransport, persistence RaftNodePersistence) *RaftNode {
	// Initialize persistent state with the provided config
	persistentState := PersistentState{
		OwnPeer:      ownPeer,
		Peers:        peers,
		Config:       config,
		CurrentTerm:  0,
		Log:          []util.LogEntry{},
		StateMachine: make(map[string]string),
		Snapshot:     util.Snapshot{},
		VotedFor:     nil,
		Persistence:  persistence,
	}

	// Initialize volatile state with default values
	volatileState := util.VolatileState{
		State:           util.ServerStateFollower,
		LastHeartbeat:   time.Now(),
		ElectionTimeout: rand.Intn(config.ElectionTimeoutMaxMs-config.ElectionTimeoutMinMs+1) + config.ElectionTimeoutMinMs,
	}

	// Initialize leader state
	leaderState := util.LeaderState{
		NextIndex:  make(map[int]int),
		MatchIndex: make(map[int]int),
	}

	loggerOptions := slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewTextHandler(os.Stdout, &loggerOptions)
	logger := slog.New(handler).With(slog.Int("nodeID", ownPeer.ID))

	rn := &RaftNode{
		transport:       transport,
		logger:          logger,
		persistentState: persistentState,
		volatileState:   volatileState,
		leaderState:     leaderState,
	}

	return rn
}

// --- Server RPCs ---

// HandleRequestVote processes a RequestVote RPC.
func (rn *RaftNode) HandleRequestVote(ctx context.Context, req util.RequestVoteRequest) util.RequestVoteResponse {
	rn.logger.Debug("Received RequestVote",
		slog.Int("candidateID", req.CandidateID),
		slog.Int("term", req.Term),
		slog.Int("myTerm", rn.persistentState.CurrentTerm),
	)

	rn.ackquireLockWithLogger("handleRequestVote")
	defer rn.releaseLockWithLogger("handleRequestVote")

	rn.resetElectionTimer()

	if req.Term < rn.persistentState.CurrentTerm {
		rn.logger.Info("Received lower term, not voting for candidate",
			slog.Int("term", req.Term),
			slog.Int("myTerm", rn.persistentState.CurrentTerm),
		)
		return util.RequestVoteResponse{
			Term:        rn.persistentState.CurrentTerm,
			VoteGranted: false,
		}
	} else if req.Term > rn.persistentState.CurrentTerm {
		rn.logger.Info("Received higher term, becoming follower",
			slog.Int("newTerm", req.Term),
			slog.Int("candidateID", req.CandidateID),
		)
		rn.becomeFollower(req.Term, &req.CandidateID)
	}

	if (rn.persistentState.VotedFor == nil || *rn.persistentState.VotedFor == req.CandidateID) &&
		rn.isLogAtLeastAsUpToDate(req.LastLogIndex, req.LastLogTerm) {
		rn.logger.Debug("Granting vote to candidate",
			slog.Int("candidateID", req.CandidateID),
			slog.Int("term", req.Term),
		)
		rn.persistentState.VotedFor = &req.CandidateID

		return util.RequestVoteResponse{
			Term:        rn.persistentState.CurrentTerm,
			VoteGranted: true,
		}
	}

	return util.RequestVoteResponse{
		Term:        rn.persistentState.CurrentTerm,
		VoteGranted: false,
	}
}

// HandleAppendEntries processes an AppendEntries RPC.
func (rn *RaftNode) HandleAppendEntries(ctx context.Context, req util.AppendEntriesRequest) util.AppendEntriesResponse {
	rn.logger.Debug("Received AppendEntries",
		slog.Int("leaderID", req.LeaderID),
	)

	rn.ackquireLockWithLogger("handleAppendEntries")
	defer rn.releaseLockWithLogger("handleAppendEntries")

	rn.resetElectionTimer()

	if req.Term < rn.persistentState.CurrentTerm {
		return util.AppendEntriesResponse{
			Term:    rn.persistentState.CurrentTerm,
			Success: false,
		}
	} else if req.Term > rn.persistentState.CurrentTerm {
		rn.logger.Info("Received higher term, becoming follower",
			slog.Int("newTerm", req.Term),
			slog.Int("leaderID", req.LeaderID),
		)
		rn.becomeFollower(req.Term, &req.LeaderID)
	} else if rn.volatileState.State == util.ServerStateCandidate {
		// Own term and term of leader are the same
		// If the node is a candidate, it should become a follower
		rn.becomeFollower(req.Term, &req.LeaderID)
	} else if rn.volatileState.CurrentLeaderID != nil && *rn.volatileState.CurrentLeaderID != req.LeaderID {
		rn.logger.Info("Received AppendEntries from different leader, becoming follower",
			slog.Int("leaderID", req.LeaderID),
		)
		rn.becomeFollower(req.Term, &req.LeaderID)
	}

	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm.
	if req.PrevLogIndex > 0 {
		if rn.persistentState.LogLength() < req.PrevLogIndex {
			rn.logger.Info("Log is too short",
				slog.Int("logLength", rn.persistentState.LogLength()),
				slog.Int("neededIndex", req.PrevLogIndex),
			)
			return util.AppendEntriesResponse{
				Term:    rn.persistentState.CurrentTerm,
				Success: false,
			}
		}

		// prevLogTerm := rn.persistentState.Log[req.PrevLogIndex-1].Term
		var prevLogTerm int
		if req.PrevLogIndex > rn.persistentState.Snapshot.LastIncludedIndex {
			prevLogTerm = rn.persistentState.Log[req.PrevLogIndex-rn.persistentState.Snapshot.LastIncludedIndex-1].Term
		} else {
			prevLogTerm = rn.persistentState.Snapshot.LastIncludedTerm
		}
		if prevLogTerm != req.PrevLogTerm {
			// Term mismatch at the expected previous index
			rn.logger.Info("Term mismatch at prevLogIndex",
				slog.Int("prevLogIndex", req.PrevLogIndex),
				slog.Int("reqPrevLogTerm", req.PrevLogTerm),
				slog.Int("prevLogTerm", prevLogTerm),
			)
			return util.AppendEntriesResponse{
				Term:    rn.persistentState.CurrentTerm,
				Success: false,
			}
		}
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	if len(req.Entries) > 0 {
		var conflictIndex int = -1

		for i, newEntry := range req.Entries {
			logIndex := req.PrevLogIndex + i + 1                                       // The absolute index of the log entry
			arrayIndex := logIndex - rn.persistentState.Snapshot.LastIncludedIndex - 1 // The index in the log array

			if arrayIndex >= 0 && arrayIndex < len(rn.persistentState.Log) {
				// This is an existing entry in our log - check for conflict
				existingEntry := rn.persistentState.Log[arrayIndex]
				if existingEntry.Term != newEntry.Term {
					// Found a conflict - different term for same index
					rn.logger.Info("Found conflict at index",
						slog.Int("logIndex", logIndex),
						slog.Int("existingTerm", existingEntry.Term),
						slog.Int("newTerm", newEntry.Term),
					)
					conflictIndex = i
					break
				}

				// Entry matches, will be replicated correctly
			} else if arrayIndex < 0 {
				// The new entry's log index is covered by the snapshot, which means a conflict.
				// This can happen if the leader's snapshot is older than ours,
				// or if it's trying to send entries that are already in our snapshot.
				rn.logger.Info("New entry at index is already covered by snapshot.", slog.Int("logIndex", logIndex))
				conflictIndex = i
			} else {
				// We've reached the end of our log - remaining entries are new
				break
			}
		}

		if conflictIndex != -1 {
			// Remove conflicting entry and everything that follows
			deleteFromIndex := req.PrevLogIndex + conflictIndex + 1                                     // The absolute index of the log entry
			deleteFromArrayIndex := deleteFromIndex - rn.persistentState.Snapshot.LastIncludedIndex - 1 // The index in the log array

			if deleteFromArrayIndex >= 0 {
				rn.logger.Debug("Truncating log from index",
					slog.Int("deleteFromIndex", deleteFromIndex),
					slog.Int("deleteFromArrayIndex", deleteFromArrayIndex),
				)
				rn.persistentState.Log = rn.persistentState.Log[:deleteFromArrayIndex]
			} else {
				// The conflict is within the snapshot range or immediately after.
				// In this case, we should discard the entire log because our log is inconsistent
				// with what the leader is sending regarding its snapshot base.
				rn.logger.Info("Truncating entire log due to conflict detected within or immediately after snapshot range.")
				rn.persistentState.Log = rn.persistentState.Log[:0]
			}
		}

		// Append any new entries not already in the log
		startAppendIndex := max(0, rn.persistentState.LogLength()-req.PrevLogIndex)
		if startAppendIndex < len(req.Entries) {
			entriesToAppend := req.Entries[startAppendIndex:]
			rn.logger.Debug("Appending entries",
				slog.Int("entriesCount", len(entriesToAppend)),
				slog.Int("logIndex", rn.persistentState.LogLength()+1),
			)
			rn.persistentState.Log = append(rn.persistentState.Log, entriesToAppend...)
		}
	}

	// Update commit index
	if req.LeaderCommit > rn.volatileState.CommitIndex {
		lastLogIndex := rn.persistentState.LogLength()
		rn.volatileState.CommitIndex = min(req.LeaderCommit, lastLogIndex)
		rn.logger.Debug("Updating commit index",
			slog.Int("commitIndex", rn.volatileState.CommitIndex),
		)

		rn.applyCommittedEntries()
	}

	return util.AppendEntriesResponse{
		Term:    rn.persistentState.CurrentTerm,
		Success: true,
	}
}

func (rn *RaftNode) HandleInstallSnapshot(request util.InstallSnapshotRequest) util.InstallSnapshotResponse {
	rn.logger.Debug("Received InstallSnapshot",
		slog.Int("leaderID", request.LeaderID),
	)

	rn.ackquireLockWithLogger("installSnapshot")
	defer rn.releaseLockWithLogger("installSnapshot")

	rn.resetElectionTimer()

	// Reply immediately if term < currentTerm
	if request.Term < rn.persistentState.CurrentTerm {
		return util.InstallSnapshotResponse{
			Term: rn.persistentState.CurrentTerm,
		}
	} else if request.Term > rn.persistentState.CurrentTerm {
		rn.logger.Info("Received higher term, becoming follower",
			slog.Int("newTerm", request.Term),
			slog.Int("leaderID", request.LeaderID),
		)
		rn.becomeFollower(request.Term, &request.LeaderID)
	}

	// Save snapshot
	oldSnapshotLastIndex := rn.persistentState.Snapshot.LastIncludedIndex
	err := rn.persistentState.Persistence.SaveSnapshot(request.Snapshot, rn.persistentState.OwnPeer.ID)
	if err != nil {
		rn.logger.Error("Failed to save snapshot", slog.String("error", err.Error()))
		return util.InstallSnapshotResponse{
			Term: rn.persistentState.CurrentTerm,
		}
	}
	rn.persistentState.Snapshot = request.Snapshot

	snapshotLastIndex := request.Snapshot.LastIncludedIndex
	snapshotLastTerm := request.Snapshot.LastIncludedTerm

	// Adjust the log based on the new snapshot
	// If an existing entry has the same index and term as the snapshot's
	// last included entry, we can keep the log entries that follow it.
	// Otherwise, we need to truncate the entire log.
	if oldSnapshotLastIndex < snapshotLastIndex {
		logIndex := oldSnapshotLastIndex + len(rn.persistentState.Log) - snapshotLastIndex

		if logIndex >= 0 && rn.persistentState.Log[logIndex].Term == snapshotLastTerm {
			rn.logger.Info("Kept log entries after installing snapshot",
				slog.Int("logIndex", logIndex),
			)
			rn.persistentState.Log = rn.persistentState.Log[:logIndex]
		} else {
			rn.logger.Info("Discarded entire log due to conflict")
			rn.persistentState.Log = rn.persistentState.Log[:0]
		}
	} else {
		rn.logger.Info("Discarded entire log since new snapshot is older")
		rn.persistentState.Log = rn.persistentState.Log[:0]
	}

	// Reset state machine using the snapshot
	rn.persistentState.StateMachine = request.Snapshot.StateMachine

	// Update commit and last applied indices to reflect the snapshot's state
	rn.volatileState.CommitIndex = snapshotLastIndex
	rn.volatileState.LastApplied = snapshotLastIndex

	rn.logger.Info("Successfully installed snapshot",
		slog.Int("lastIncludedIndex", snapshotLastIndex),
	)
	return util.InstallSnapshotResponse{
		Term: rn.persistentState.CurrentTerm,
	}
}

// -- Client RPCs ---

func (rn *RaftNode) Put(ctx context.Context, request util.PutRequest) (util.PutResponse, error) {
	rn.mu.RLock()
	if rn.volatileState.State != util.ServerStateLeader {
		rn.mu.RUnlock()
		return util.PutResponse{
			Success:    false,
			LeaderHint: rn.firstPeerWithID(*rn.volatileState.CurrentLeaderID),
		}, nil
	}
	term := rn.persistentState.CurrentTerm
	rn.mu.RUnlock()

	err := rn.replicateLog([]util.LogEntry{
		{
			Term:  term,
			Key:   &request.Key,
			Value: request.Value,
		},
	})

	if err != nil {
		return util.PutResponse{
			Success: false,
		}, err
	}

	return util.PutResponse{
		Success: true,
	}, nil
}

func (rn *RaftNode) Get(ctx context.Context, request util.GetRequest) util.GetResponse {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.volatileState.State != util.ServerStateLeader {
		return util.GetResponse{
			LeaderHint: rn.firstPeerWithID(*rn.volatileState.CurrentLeaderID),
		}
	}

	if val, ok := rn.persistentState.StateMachine[request.Key]; ok {
		return util.GetResponse{
			Value: &val,
		}
	}

	return util.GetResponse{}
}

func (rn *RaftNode) GetDebug(ctx context.Context, request util.GetRequest) util.GetResponse {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if val, ok := rn.persistentState.StateMachine[request.Key]; ok {
		return util.GetResponse{
			Value: &val,
		}
	}

	return util.GetResponse{}
}

func (rn *RaftNode) GetState(ctx context.Context) util.ServerStateResponse {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	return util.ServerStateResponse{
		ID:    rn.persistentState.OwnPeer.ID,
		State: rn.volatileState.State,
	}
}

func (rn *RaftNode) GetTerm(ctx context.Context) util.ServerTermResponse {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	return util.ServerTermResponse{
		ID:   rn.persistentState.OwnPeer.ID,
		Term: rn.persistentState.CurrentTerm,
	}
}

func (rn *RaftNode) GetImplementationVersion(ctx context.Context) util.ImplementationVersionResponse {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	return util.ImplementationVersionResponse{
		ID:             rn.persistentState.OwnPeer.ID,
		Implementation: "Go",
		Version:        "1.3.0",
	}
}

// -- Node Lifecycle --

// Start starts the node.
func (rn *RaftNode) Start() {
	rn.loadSnapshotOnStartup()
	rn.startHeartbeatTask()
}

// Shutdown shuts down the node.
func (rn *RaftNode) Shutdown() {
	rn.stopHeartbeatTask()
	// Wait for all goroutines to finish
	rn.wg.Wait()
}

// Starts the heartbeat task.
//
// If the node is a leader, it will send heartbeats to all followers.
// If the node is a follower, it will check the election timeout.
func (rn *RaftNode) startHeartbeatTask() {
	// Cancel existing task if it exists
	rn.stopHeartbeatTask()

	rn.heartbeatMu.Lock()
	defer rn.heartbeatMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	rn.heartbeatCancel = cancel

	rn.wg.Add(1)
	go func() {
		defer rn.wg.Done()
		rn.heartbeatLoop(ctx)
	}()
}

func (rn *RaftNode) stopHeartbeatTask() {
	rn.heartbeatMu.Lock()
	defer rn.heartbeatMu.Unlock()

	if rn.heartbeatCancel != nil {
		rn.heartbeatCancel()
		rn.heartbeatCancel = nil
	}
}

func (rn *RaftNode) heartbeatLoop(ctx context.Context) {
	rn.mu.RLock()
	heartbeatInterval := rn.persistentState.Config.HeartbeatIntervalMs
	rn.mu.RUnlock()

	for {
		var timeout time.Duration
		var action func() error

		rn.mu.RLock()
		state := rn.volatileState.State
		rn.mu.RUnlock()

		if state == util.ServerStateLeader {
			timeout = time.Duration(heartbeatInterval) * time.Millisecond
			action = rn.sendHeartbeat
		} else {
			timeout = time.Duration(heartbeatInterval) * time.Millisecond
			action = rn.checkElectionTimeout
		}

		// Create a context with timeout for the action
		_, actionCancel := context.WithCancel(ctx)

		// Run the action in a separate goroutine
		go func() {
			defer actionCancel()
			if err := action(); err != nil {
			}
		}()

		// Sleep for the timeout duration
		select {
		case <-ctx.Done():
			actionCancel()
			return
		case <-time.After(timeout):
			// Timeout reached, cancel action if still running
			actionCancel()
			// Continue to next iteration
		}
	}
}

// Sends a heartbeat to all followers.
func (rn *RaftNode) sendHeartbeat() error {
	rn.mu.RLock()
	state := rn.volatileState.State
	rn.mu.RUnlock()

	if state != util.ServerStateLeader {
		return util.NotLeaderError
	}

	rn.logger.Debug("Sending heartbeat to followers")
	return rn.replicateLog([]util.LogEntry{})
}

// Checks if the election timeout has been reached.
func (rn *RaftNode) checkElectionTimeout() error {
	rn.mu.RLock()
	lastHeartbeat := rn.volatileState.LastHeartbeat
	electionTimeout := rn.volatileState.ElectionTimeout
	rn.mu.RUnlock()

	now := time.Now()
	timeSinceLastHeartbeat := now.Sub(lastHeartbeat)

	if timeSinceLastHeartbeat.Milliseconds() > int64(electionTimeout) {
		rn.logger.Info("Election timeout reached, becoming candidate")
		return rn.startElection()
	}

	return nil
}

// -- Election --

// Reset the election timer.
func (rn *RaftNode) resetElectionTimer() {
	rn.volatileState.LastHeartbeat = time.Now()
}

// Starts an election.
func (rn *RaftNode) startElection() error {
	rn.logger.Debug("Starting election")
	rn.ackquireLockWithLogger("startElection")
	rn.becomeCandidate()

	// Reset election timeout
	rn.volatileState.ElectionTimeout = rand.Intn(rn.persistentState.Config.ElectionTimeoutMaxMs-rn.persistentState.Config.ElectionTimeoutMinMs+1) + rn.persistentState.Config.ElectionTimeoutMinMs
	rn.resetElectionTimer()
	rn.releaseLockWithLogger("startElection")

	// Request votes from other nodes
	return rn.requestVotes()
}

type VoteResult struct {
	peerId int
	vote   util.RequestVoteResponse
}

// Requests votes from all peers.
func (rn *RaftNode) requestVotes() error {
	rn.mu.RLock()
	rn.logger.Debug("Requesting votes",
		slog.Any("peers", rn.persistentState.Peers),
	)

	// Count own vote
	var votes = 1
	var requiredVotes = rn.Majority()

	currentTerm := rn.persistentState.CurrentTerm
	candidateID := rn.persistentState.OwnPeer.ID
	lastLogIndex := rn.persistentState.LogLength()
	lastLogTerm := rn.persistentState.Snapshot.LastIncludedTerm
	if len(rn.persistentState.Log) > 0 {
		lastLogTerm = rn.persistentState.Log[len(rn.persistentState.Log)-1].Term
	}
	peers := rn.persistentState.Peers
	rn.mu.RUnlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel to collect vote responses
	voteResults := make(chan VoteResult, len(peers))

	// Start vote requests concurrently
	var wg sync.WaitGroup
	wg.Add(len(peers))

	for _, peer := range peers {
		go func(peer util.Peer) {
			defer wg.Done()

			voteRequest := util.RequestVoteRequest{
				Term:         currentTerm,
				CandidateID:  candidateID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			peerId, vote := rn.requestVoteFromPeer(ctx, peer, voteRequest)

			select {
			case voteResults <- VoteResult{peerId: peerId, vote: vote}:
			case <-ctx.Done():
			}
		}(peer)
	}

	// Close channel when all goroutines are done
	go func() {
		wg.Wait()
		close(voteResults)
	}()

	// Process vote responses
	for result := range voteResults {
		rn.logger.Debug("Received vote",
			slog.Int("peerId", result.peerId),
			slog.Bool("voteGranted", result.vote.VoteGranted),
		)

		rn.ackquireLockWithLogger("requestVotes")

		// Check if the peer has a higher term
		if result.vote.Term > rn.persistentState.CurrentTerm {
			rn.logger.Info("Received higher term, becoming follower",
				slog.Int("term", result.vote.Term),
				slog.Int("peerId", result.peerId),
			)
			rn.becomeFollower(result.vote.Term, &result.peerId)
			rn.releaseLockWithLogger("requestVotes")
			// Cancel remaining vote requests
			cancel()
			return nil
		}

		// Count votes only if we're still a candidate and in the same term
		if rn.volatileState.State == util.ServerStateCandidate && result.vote.Term == rn.persistentState.CurrentTerm && result.vote.VoteGranted {
			votes++

			if votes >= requiredVotes {
				rn.logger.Info("Received majority of votes, becoming leader",
					slog.Int("votes", votes),
					slog.Int("numberOfPeers", len(peers)+1),
				)
				rn.becomeLeader()
				rn.releaseLockWithLogger("requestVotes")
				// Cancel remaining vote requests
				cancel()
				return nil
			}
		}

		rn.releaseLockWithLogger("requestVotes")
	}

	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.volatileState.State == util.ServerStateCandidate {
		rn.logger.Info("Election failed",
			slog.Int("votes", votes),
			slog.Int("numberOfPeers", len(peers)+1),
		)
	}

	return nil
}

func (rn *RaftNode) requestVoteFromPeer(
	ctx context.Context,
	peer util.Peer,
	voteRequest util.RequestVoteRequest,
) (int, util.RequestVoteResponse) {
	rn.logger.Debug("Requesting vote",
		slog.Int("peerId", peer.ID),
	)

	response, err := rn.transport.RequestVote(ctx, voteRequest, peer)
	if err != nil {
		rn.logger.Error("Failed to request vote",
			slog.Int("peerId", peer.ID),
			slog.Any("error", err),
		)
		return peer.ID, util.RequestVoteResponse{
			Term:        0,
			VoteGranted: false,
		}
	}

	return peer.ID, response
}

// -- Log Replication --

func (rn *RaftNode) replicateLog(entries []util.LogEntry) error {
	rn.ackquireLockWithLogger("replicateLog 1")

	if rn.volatileState.State != util.ServerStateLeader {
		rn.logger.Info("Not a leader, not replicating log")
		rn.releaseLockWithLogger("replicateLog 1")
		return util.NotLeaderError
	}

	rn.resetElectionTimer()

	currentTerm := rn.persistentState.CurrentTerm
	leaderID := rn.persistentState.OwnPeer.ID
	peers := rn.persistentState.Peers
	commitIndex := rn.volatileState.CommitIndex
	originalLogLength := rn.persistentState.LogLength()

	// Add log entries to log
	rn.persistentState.Log = append(rn.persistentState.Log, entries...)
	rn.releaseLockWithLogger("replicateLog 1")

	majority := rn.Majority()

	replicationTracker := util.NewReplicationTracker(majority)
	rn.logger.Debug("Replicating log entries to peers",
		slog.Int("majority", majority),
		slog.Any("entries", entries),
	)

	replicationTracker.MarkSuccess(leaderID)

	ctx, cancel := context.WithCancel(context.Background())

	// Start replication to all peers concurrently
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer util.Peer) {
			defer wg.Done()

			err := rn.replicateLogToPeer(ctx, peer, replicationTracker, currentTerm, leaderID, commitIndex, originalLogLength, entries)
			if err != nil {
				rn.logger.Error("Failed to replicate to peer",
					slog.Int("peerId", peer.ID),
					slog.Any("error", err),
				)
			}
		}(peer)
	}

	// Wait for replication to complete in background
	go func() {
		wg.Wait()
		cancel()
	}()

	// Wait for majority to have replicated the log before returning
	replicationTracker.WaitForMajority(ctx)
	rn.logger.Debug("Majority of peers have replicated the log",
		slog.Int("majority", majority),
		slog.Any("entries", entries),
	)

	// Once majority has replicated the log, update commit index and apply committed entries
	rn.mu.RLock()
	state := rn.volatileState.State
	term := rn.persistentState.CurrentTerm
	rn.mu.RUnlock()

	if state == util.ServerStateLeader && term == currentTerm {
		rn.updateCommitIndexAndApply()
	}

	return nil
}

// replicateLogToPeer replicates log entries to a single peer.
func (rn *RaftNode) replicateLogToPeer(
	ctx context.Context,
	peer util.Peer,
	replicationTracker *util.ReplicationTracker,
	currentTerm int,
	leaderID int,
	commitIndex int,
	originalLogLength int,
	entries []util.LogEntry,
) error {
	retryCount := 0
	targetEndIndex := originalLogLength + len(entries)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rn.ackquireLockWithLogger("replicateLogToPeer 1")

		// Check conditions for continuing
		if rn.volatileState.State != util.ServerStateLeader ||
			rn.persistentState.CurrentTerm != currentTerm ||
			rn.firstPeerWithID(peer.ID) == nil {
			rn.releaseLockWithLogger("replicateLogToPeer 1")
			return nil
		}
		rn.releaseLockWithLogger("replicateLogToPeer 1")

		// Check if already successful
		if replicationTracker.IsSuccessful(peer.ID) {
			return nil
		}

		rn.ackquireLockWithLogger("replicateLogToPeer 2")
		currentMatchIndex := rn.leaderState.MatchIndex[peer.ID]
		if currentMatchIndex > targetEndIndex {
			rn.releaseLockWithLogger("replicateLogToPeer 2")
			replicationTracker.MarkSuccess(peer.ID)
			return nil
		}

		// Determine peer's next index
		peerNextIndex := rn.leaderState.NextIndex[peer.ID]
		if peerNextIndex == 0 {
			peerNextIndex = len(rn.persistentState.Log) + 1
		}

		// Check if peer needs a snapshot
		if peerNextIndex <= rn.persistentState.Snapshot.LastIncludedIndex {
			rn.releaseLockWithLogger("replicateLogToPeer 2")
			// Peer is too far behind, send snapshot
			rn.logger.Info("Peer is too far behind, sending snapshot", "snapshotLastIncludedIndex",
				slog.Int("peerID", peer.ID),
				slog.Int("peerNextIndex", peerNextIndex),
				slog.Int("snapshotLastIncludedIndex", rn.persistentState.Snapshot.LastIncludedIndex),
			)
			rn.sendSnapshotToPeer(peer)

			// After sending snapshot, update nextIndex and matchIndex
			rn.leaderState.NextIndex[peer.ID] = rn.persistentState.Snapshot.LastIncludedIndex + 1
			rn.leaderState.MatchIndex[peer.ID] = rn.persistentState.Snapshot.LastIncludedIndex

			// Continue to next iteration to try sending append entries from the new nextIndex
			continue
		}

		// Calculate entries to send
		var entriesToSend []util.LogEntry
		if peerNextIndex <= originalLogLength {
			// Peer needs entries from the original log (catch-up scenario)
			startIndex := peerNextIndex - rn.persistentState.Snapshot.LastIncludedIndex - 1
			entriesToSend = rn.persistentState.Log[startIndex:]
		} else if peerNextIndex == originalLogLength+1 {
			// Peer is up-to-date with original log, send only new entries
			entriesToSend = entries
		} else {
			// Peer's nextIndex is beyond what we expect - reset and retry
			rn.logger.Warn("Peer's nextIndex is beyond what we expect",
				slog.Int("peerID", peer.ID),
				slog.Int("peerNextIndex", peerNextIndex),
				slog.Int("originalLogLength", originalLogLength),
			)
			rn.leaderState.NextIndex[peer.ID] = originalLogLength + 1
			rn.releaseLockWithLogger("replicateLogToPeer 2")
			continue
		}

		peerPrevLogIndex := peerNextIndex - 1
		peerPrevLogTerm := rn.persistentState.Snapshot.LastIncludedTerm
		if peerPrevLogIndex > rn.persistentState.Snapshot.LastIncludedIndex {
			peerPrevLogTerm = rn.persistentState.Log[peerPrevLogIndex-rn.persistentState.Snapshot.LastIncludedIndex-1].Term
		}

		rn.releaseLockWithLogger("replicateLogToPeer 2")

		rn.logger.Debug("Sending append entries",
			slog.Int("peerId", peer.ID),
			slog.Int("nextIndex", peerNextIndex),
			slog.Int("prevLogIndex", peerPrevLogIndex),
			slog.Int("prevLogTerm", peerPrevLogTerm),
			slog.Int("entriesCount", len(entriesToSend)),
		)

		appendEntriesRequest := util.AppendEntriesRequest{
			Term:         currentTerm,
			LeaderID:     leaderID,
			PrevLogIndex: peerPrevLogIndex,
			PrevLogTerm:  peerPrevLogTerm,
			Entries:      entriesToSend,
			LeaderCommit: commitIndex,
		}

		result, err := rn.transport.AppendEntries(ctx, appendEntriesRequest, peer)
		if err != nil {
			retryCount++
			errorMsg := fmt.Sprintf("Failed to replicate log to peer %d: %v", peer.ID, err)
			if retryCount == 1 {
				rn.logger.Error(errorMsg, "retryCount", retryCount)
			} else {
				rn.logger.Debug(errorMsg, "retryCount", retryCount)
			}

			// Wait before retrying with exponential backoff
			backoffDuration := time.Duration(100*min(64, 1<<retryCount)) * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoffDuration):
				continue
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rn.ackquireLockWithLogger("replicateLogToPeer 3")

		if result.Term > rn.persistentState.CurrentTerm {
			rn.logger.Info("Received higher term, becoming follower",
				slog.Int("term", result.Term),
				slog.Int("peerId", peer.ID),
			)
			rn.becomeFollower(result.Term, &peer.ID)
			rn.releaseLockWithLogger("replicateLogToPeer 3")
			return nil
		}

		if result.Success {
			replicationTracker.MarkSuccess(peer.ID)
			rn.logger.Debug("Append entries successful",
				slog.Int("peerId", peer.ID))

			newMatchIndex := peerPrevLogIndex + len(entriesToSend)
			rn.leaderState.MatchIndex[peer.ID] = newMatchIndex
			rn.leaderState.NextIndex[peer.ID] = newMatchIndex + 1

			rn.releaseLockWithLogger("replicateLogToPeer 3")
			return nil
		} else {
			// Log inconsistency, decrement nextIndex and retry
			rn.leaderState.NextIndex[peer.ID] = max(1, rn.leaderState.NextIndex[peer.ID]-1)
			rn.releaseLockWithLogger("replicateLogToPeer 3")
			retryCount++

			rn.logger.Info("Append entries failed, retrying with earlier index",
				slog.Int("peerId", peer.ID),
				slog.Int("nextIndex", rn.leaderState.NextIndex[peer.ID]),
				slog.Int("retryCount", retryCount),
			)

			// Wait before retrying with exponential backoff
			backoffDuration := time.Duration(100*min(64, 1<<retryCount)) * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoffDuration):
				continue
			}
		}
	}
}

func (rn *RaftNode) updateCommitIndexAndApply() {
	rn.mu.RLock()

	// Safety check: ensure we are still leader
	if rn.volatileState.State != util.ServerStateLeader {
		rn.mu.RUnlock()
		return
	}

	// Add own match index (implicitly the end of the log)
	allMatchIndices := make(map[int]int)
	maps.Copy(allMatchIndices, rn.leaderState.MatchIndex)
	allMatchIndices[rn.persistentState.OwnPeer.ID] = rn.persistentState.LogLength()

	rn.mu.RUnlock()

	// Calculate new commit index based on majority match indices
	indices := make([]int, 0, len(allMatchIndices))
	for _, index := range allMatchIndices {
		indices = append(indices, index)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(indices)))

	majorityIndex := indices[rn.Majority()-1]
	rn.ackquireLockWithLogger("updateCommitIndexAndApply 1")
	defer rn.releaseLockWithLogger("updateCommitIndexAndApply 1")
	oldCommitIndex := rn.volatileState.CommitIndex

	// Only update commit index if it's in the current term
	for newCommitIndex := majorityIndex; newCommitIndex > oldCommitIndex; newCommitIndex-- {
		if newCommitIndex <= rn.persistentState.Snapshot.LastIncludedIndex {
			// This index is covered by the snapshot, so it's already committed
			// Don't update commitIndex as it should already be at least this value
			break
		}

		// Calculate the array index in the current log
		logArrayIndex := newCommitIndex - rn.persistentState.Snapshot.LastIncludedIndex - 1

		// Ensure the index is within bounds of the current log
		if logArrayIndex >= 0 && logArrayIndex < len(rn.persistentState.Log) {
			entry := rn.persistentState.Log[logArrayIndex]
			// Only commit entries from the current term for safety
			if entry.Term == rn.persistentState.CurrentTerm {
				rn.volatileState.CommitIndex = newCommitIndex
				rn.logger.Debug("Updated commit index",
					slog.Int("oldCommitIndex", oldCommitIndex),
					slog.Int("newCommitIndex", newCommitIndex))
				break
			}
			// If the entry is from an older term, continue checking lower indices
		} else {
			// Index is out of bounds - this shouldn't happen with correct logic
			rn.logger.Warn("Calculated newCommitIndex is out of bounds for log",
				slog.Int("newCommitIndex", newCommitIndex),
				slog.Int("logLength", len(rn.persistentState.Log)),
				slog.Int("snapshotLastIncludedIndex", rn.persistentState.Snapshot.LastIncludedIndex),
			)
		}
	}

	// Apply newly committed entries to state machine
	rn.applyCommittedEntries()
}

// Applies the committed entries to the state machine.
func (rn *RaftNode) applyCommittedEntries() {
	for rn.volatileState.LastApplied < rn.volatileState.CommitIndex {
		nextApplyIndex := rn.volatileState.LastApplied + 1

		if nextApplyIndex <= rn.persistentState.Snapshot.LastIncludedIndex {
			rn.volatileState.LastApplied = nextApplyIndex
			continue
		}

		logArrayIndex := nextApplyIndex - rn.persistentState.Snapshot.LastIncludedIndex - 1

		if logArrayIndex >= 0 && logArrayIndex < len(rn.persistentState.Log) {
			entry := rn.persistentState.Log[logArrayIndex]

			if entry.Key != nil {
				oldValue := rn.persistentState.StateMachine[*entry.Key]
				rn.persistentState.StateMachine[*entry.Key] = *entry.Value

				rn.logger.Debug("Applied entry",
					slog.Int("index", rn.volatileState.LastApplied+1),
					slog.String("key", *entry.Key),
					slog.String("value", *entry.Value),
					slog.String("previousValue", oldValue),
				)
			}
		} else {
			rn.logger.Warn("Cannot apply entry at index: log array index is out of bounds",
				slog.Int("index", nextApplyIndex),
				slog.Int("logLength", len(rn.persistentState.Log)),
				slog.Int("logArrayIndex", logArrayIndex),
				slog.Int("snapshotLastIncludedIndex", rn.persistentState.Snapshot.LastIncludedIndex),
			)
		}

		rn.volatileState.LastApplied++
	}

	go func() {
		if rn.shouldCreateSnapshot() {
			rn.createSnapshot()
		}
	}()
}

// Checks if the node's log is at least as up-to-date as the given log.
//
// - Parameters:
//   - lastLogIndex: The index of the last log entry.
//   - lastLogTerm: The term of the last log entry.
//
// - Returns: True if the node's log is at least as up-to-date as the given log.
func (rn *RaftNode) isLogAtLeastAsUpToDate(lastLogIndex int, lastLogTerm int) bool {
	localLastLogTerm := rn.persistentState.Snapshot.LastIncludedTerm
	if len(rn.persistentState.Log) > 0 {
		localLastLogTerm = rn.persistentState.Log[len(rn.persistentState.Log)-1].Term
	}

	if lastLogTerm != localLastLogTerm {
		return lastLogTerm > localLastLogTerm
	}

	localLastLogIndex := len(rn.persistentState.Log)
	return lastLogIndex >= localLastLogIndex
}

// -- Snapshotting --

// Loads the snapshot on startup of the node.
func (rn *RaftNode) loadSnapshotOnStartup() {
	snapshot, err := rn.persistentState.Persistence.LoadSnapshot(rn.persistentState.OwnPeer.ID)
	if err != nil {
		rn.logger.Error("Failed to load snapshot", slog.Any("error", err))
		return
	}

	if snapshot != nil {
		rn.ackquireLockWithLogger("loadSnapshotOnStartup 1")
		defer rn.releaseLockWithLogger("loadSnapshotOnStartup 1")

		rn.persistentState.Snapshot = *snapshot
		rn.persistentState.CurrentTerm = snapshot.LastIncludedTerm
		rn.persistentState.StateMachine = snapshot.StateMachine
		rn.volatileState.CommitIndex = snapshot.LastIncludedIndex
		rn.volatileState.LastApplied = snapshot.LastIncludedIndex
		rn.logger.Info("Successfully loaded snapshot",
			slog.Int("lastIncludedIndex", snapshot.LastIncludedIndex),
			slog.Int("lastIncludedTerm", snapshot.LastIncludedTerm),
		)
	}
}

// Checks if a snapshot should be created.
func (rn *RaftNode) shouldCreateSnapshot() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.persistentState.Persistence.CompactionThreshold() <= 0 {
		return false
	}

	return (rn.volatileState.CommitIndex-rn.persistentState.Snapshot.LastIncludedIndex) >= rn.persistentState.Persistence.CompactionThreshold() && !rn.persistentState.IsSnapshotting
}

// Creates a snapshot.
func (rn *RaftNode) createSnapshot() {
	rn.ackquireLockWithLogger("createSnapshot 1")
	if rn.persistentState.IsSnapshotting {
		rn.releaseLockWithLogger("createSnapshot 1")
		return
	}

	rn.persistentState.IsSnapshotting = true

	snapshotLastIndex := rn.volatileState.CommitIndex
	if snapshotLastIndex <= rn.persistentState.Snapshot.LastIncludedIndex {
		rn.persistentState.IsSnapshotting = false
		rn.releaseLockWithLogger("createSnapshot 1")
		return
	}

	lastCommittedArrayIndex := snapshotLastIndex - rn.persistentState.Snapshot.LastIncludedIndex - 1
	if lastCommittedArrayIndex < 0 || lastCommittedArrayIndex >= len(rn.persistentState.Log) {
		rn.persistentState.IsSnapshotting = false
		rn.releaseLockWithLogger("createSnapshot 1")
		return
	}

	snapshotLastTerm := rn.persistentState.Log[lastCommittedArrayIndex].Term

	// Deep copy the state machine to avoid concurrent access
	stateMachineCopy := make(map[string]string)
	maps.Copy(stateMachineCopy, rn.persistentState.StateMachine)

	snapshot := util.Snapshot{
		LastIncludedIndex: snapshotLastIndex,
		LastIncludedTerm:  snapshotLastTerm,
		StateMachine:      stateMachineCopy,
	}
	rn.releaseLockWithLogger("createSnapshot 1")

	err := rn.persistentState.Persistence.SaveSnapshot(snapshot, rn.persistentState.OwnPeer.ID)

	rn.ackquireLockWithLogger("createSnapshot 2")
	defer rn.releaseLockWithLogger("createSnapshot 2")
	defer func() {
		rn.persistentState.IsSnapshotting = false
	}()

	if err != nil {
		rn.logger.Error("Failed to save snapshot", slog.Any("error", err))
		return
	}

	rn.persistentState.Snapshot = snapshot

	// Truncate the log after saving snapshot
	entriesToKeep := lastCommittedArrayIndex + 1
	rn.persistentState.Log = rn.persistentState.Log[entriesToKeep:]

	rn.logger.Info("Successfully created snapshot",
		slog.Int("lastIncludedIndex", snapshotLastIndex),
		slog.Int("lastIncludedTerm", snapshotLastTerm),
	)
}

// Sends a snapshot to a specific peer.
func (rn *RaftNode) sendSnapshotToPeer(peer util.Peer) {
	rn.mu.RLock()

	if rn.volatileState.State != util.ServerStateLeader {
		rn.mu.RUnlock()
		return
	}

	currentTerm := rn.persistentState.CurrentTerm
	leaderID := rn.persistentState.OwnPeer.ID
	snapshot := rn.persistentState.Snapshot
	rn.mu.RUnlock()

	rn.logger.Info("Sending InstallSnapshot RPC to peer",
		slog.Int("peerID", peer.ID),
		slog.Int("lastIncludedIndex", snapshot.LastIncludedIndex),
		slog.Int("lastIncludedTerm", snapshot.LastIncludedTerm),
	)

	request := util.InstallSnapshotRequest{
		Term:     currentTerm,
		LeaderID: leaderID,
		Snapshot: snapshot,
	}

	response, err := rn.transport.InstallSnapshot(context.Background(), request, peer)
	if err != nil {
		rn.logger.Error("Failed to send snapshot to peer",
			slog.Any("error", err),
			slog.Int("peerID", peer.ID),
		)
		return
	}

	rn.ackquireLockWithLogger("sendSnapshotToPeer 1")
	defer rn.releaseLockWithLogger("sendSnapshotToPeer 1")

	if response.Term > rn.persistentState.CurrentTerm {
		rn.logger.Info("Received higher term during InstallSnapshot, becoming follower",
			slog.Int("peerID", peer.ID),
			slog.Int("term", response.Term),
		)
		rn.becomeFollower(response.Term, &peer.ID)
		return
	}

	// Upon successful installation, update matchIndex and nextIndex for the peer
	rn.leaderState.MatchIndex[peer.ID] = snapshot.LastIncludedIndex
	rn.leaderState.NextIndex[peer.ID] = snapshot.LastIncludedIndex + 1
	rn.logger.Info("Successfully sent snapshot to peer",
		slog.Int("peerID", peer.ID),
		slog.Int("lastIncludedIndex", snapshot.LastIncludedIndex),
		slog.Int("lastIncludedTerm", snapshot.LastIncludedTerm),
	)
}

// -- State Changes --

// Become follower.
//
// - Parameters:
//   - newTerm: The new term.
//   - currentLeaderId: The ID of the current leader.
func (rn *RaftNode) becomeFollower(newTerm int, currentLeaderId *int) {
	oldState := rn.volatileState.State
	if oldState != util.ServerStateFollower {
		rn.logger.Info(fmt.Sprintf("Transitioning from %s to follower for term %d", oldState, newTerm))
	}

	rn.volatileState.State = util.ServerStateFollower
	rn.persistentState.CurrentTerm = newTerm
	rn.persistentState.VotedFor = nil
	rn.volatileState.CurrentLeaderID = currentLeaderId
	rn.resetElectionTimer()
}

// Become candidate.
func (rn *RaftNode) becomeCandidate() {
	rn.logger.Info(fmt.Sprintf("Transitioning to candidate for term %d", rn.persistentState.CurrentTerm+1))

	rn.volatileState.State = util.ServerStateCandidate
	rn.persistentState.CurrentTerm += 1
	rn.persistentState.VotedFor = &rn.persistentState.OwnPeer.ID
	rn.volatileState.CurrentLeaderID = nil
}

// Become leader.
func (rn *RaftNode) becomeLeader() {
	rn.logger.Info(fmt.Sprintf("Transitioning to leader for term %d", rn.persistentState.CurrentTerm))

	rn.volatileState.State = util.ServerStateLeader
	rn.volatileState.CurrentLeaderID = &rn.persistentState.OwnPeer.ID

	// Clear and reinitialize maps
	rn.leaderState.NextIndex = make(map[int]int)
	rn.leaderState.MatchIndex = make(map[int]int)

	// Initialize nextIndex and matchIndex for all peers
	for _, peer := range rn.persistentState.Peers {
		rn.leaderState.NextIndex[peer.ID] = len(rn.persistentState.Log) + 1
		rn.leaderState.MatchIndex[peer.ID] = 0
	}
}

// -- Utilities --

func (rn *RaftNode) firstPeerWithID(id int) *util.Peer {
	for _, peer := range rn.persistentState.Peers {
		if peer.ID == id {
			return &peer
		}
	}

	return nil
}

func (rn *RaftNode) ackquireLockWithLogger(location string) {
	rn.logger.Debug("Locking", slog.String("location", location))
	rn.mu.Lock()
	rn.logger.Debug("Lock acquired", slog.String("location", location))
}

func (rn *RaftNode) releaseLockWithLogger(location string) {
	rn.logger.Debug("Releasing lock", slog.String("location", location))
	rn.mu.Unlock()
}
