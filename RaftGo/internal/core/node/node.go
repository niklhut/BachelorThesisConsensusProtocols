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
	persistentState util.PersistentState
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
func NewRaftNode(ownPeer util.Peer, peers []util.Peer, config util.RaftConfig, transport RaftPeerTransport) *RaftNode {
	// Initialize persistent state with the provided config
	persistentState := util.PersistentState{
		OwnPeer:      ownPeer,
		Peers:        peers,
		Config:       config,
		CurrentTerm:  0,
		Log:          []util.LogEntry{},
		StateMachine: make(map[string]string),
		Snapshot:     util.Snapshot{},
		VotedFor:     nil,
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
		if len(rn.persistentState.Log) < req.PrevLogIndex {
			rn.logger.Info("Log is too short",
				slog.Int("logLength", len(rn.persistentState.Log)),
				slog.Int("neededIndex", req.PrevLogIndex),
			)
			return util.AppendEntriesResponse{
				Term:    rn.persistentState.CurrentTerm,
				Success: false,
			}
		}

		prevLogTerm := rn.persistentState.Log[req.PrevLogIndex-1].Term
		if prevLogTerm != req.PrevLogTerm {
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
			logIndex := req.PrevLogIndex + i + 1
			arrayIndex := logIndex - 1

			if arrayIndex < len(rn.persistentState.Log) {
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
			} else {
				// We've reached the end of our log - remaining entries are new
				break
			}
		}

		if conflictIndex != -1 {
			// Remove conflicting entry and everything that follows
			deleteFromIndex := req.PrevLogIndex + conflictIndex + 1
			deleteFromArrayIndex := deleteFromIndex - 1

			rn.logger.Debug("Truncating log from index",
				slog.Int("deleteFromIndex", deleteFromIndex))
			rn.persistentState.Log = rn.persistentState.Log[:deleteFromArrayIndex]
		}

		// Append any new entries not already in the log
		startAppendIndex := max(0, len(rn.persistentState.Log)-req.PrevLogIndex)
		if startAppendIndex < len(req.Entries) {
			entriesToAppend := req.Entries[startAppendIndex:]
			rn.logger.Debug("Appending entries",
				slog.Int("entriesCount", len(entriesToAppend)),
				slog.Int("logIndex", len(rn.persistentState.Log)+1),
			)
			rn.persistentState.Log = append(rn.persistentState.Log, entriesToAppend...)
		}
	}

	// Update commit index
	if req.LeaderCommit > rn.volatileState.CommitIndex {
		lastLogIndex := len(rn.persistentState.Log)
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

// -- Node Lifecycle --

// Start starts the node.
func (rn *RaftNode) Start() {
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
	for {
		var timeout time.Duration
		var action func() error

		rn.mu.RLock()
		state := rn.volatileState.State
		heartbeatInterval := rn.persistentState.Config.HeartbeatIntervalMs
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

	// Create a deep copy snapshot of the persistent state to avoid race conditions
	persistentStateSnapshot := util.PersistentState{
		OwnPeer:      rn.persistentState.OwnPeer,
		Peers:        make([]util.Peer, len(rn.persistentState.Peers)),
		Config:       rn.persistentState.Config,
		CurrentTerm:  rn.persistentState.CurrentTerm,
		Log:          make([]util.LogEntry, len(rn.persistentState.Log)),
		StateMachine: make(map[string]string),
		Snapshot:     rn.persistentState.Snapshot,
		VotedFor:     rn.persistentState.VotedFor,
	}
	copy(persistentStateSnapshot.Peers, rn.persistentState.Peers)
	copy(persistentStateSnapshot.Log, rn.persistentState.Log)
	maps.Copy(persistentStateSnapshot.StateMachine, rn.persistentState.StateMachine)
	rn.mu.RUnlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel to collect vote responses
	voteResults := make(chan VoteResult, len(rn.persistentState.Peers))

	// Start vote requests concurrently
	var wg sync.WaitGroup
	wg.Add(len(rn.persistentState.Peers))

	for _, peer := range rn.persistentState.Peers {
		go func(peer util.Peer) {
			defer wg.Done()

			peerId, vote := rn.requestVoteFromPeer(ctx, peer, persistentStateSnapshot)

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
			rn.mu.Unlock()
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
					slog.Int("numberOfPeers", len(rn.persistentState.Peers)+1),
				)
				rn.becomeLeader()
				rn.mu.Unlock()
				// Cancel remaining vote requests
				cancel()
				return nil
			}
		}

		rn.mu.Unlock()
	}

	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.volatileState.State == util.ServerStateCandidate {
		rn.logger.Info("Election failed",
			slog.Int("votes", votes),
			slog.Int("numberOfPeers", len(rn.persistentState.Peers)+1),
		)
	}

	return nil
}

func (rn *RaftNode) requestVoteFromPeer(ctx context.Context, peer util.Peer, persistentStateSnapshot util.PersistentState) (int, util.RequestVoteResponse) {
	rn.logger.Debug("Requesting vote",
		slog.Int("peerId", peer.ID),
	)

	voteRequest := util.RequestVoteRequest{
		Term:         persistentStateSnapshot.CurrentTerm,
		CandidateID:  persistentStateSnapshot.OwnPeer.ID,
		LastLogIndex: len(persistentStateSnapshot.Log),
		LastLogTerm:  0, // Set it below
	}

	if len(persistentStateSnapshot.Log) > 0 {
		voteRequest.LastLogTerm = persistentStateSnapshot.Log[len(persistentStateSnapshot.Log)-1].Term
	}

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

	// Create deep copies to avoid race conditions
	persistentStateSnapshot := util.PersistentState{
		OwnPeer:      rn.persistentState.OwnPeer,
		Peers:        make([]util.Peer, len(rn.persistentState.Peers)),
		Config:       rn.persistentState.Config,
		CurrentTerm:  rn.persistentState.CurrentTerm,
		Log:          make([]util.LogEntry, len(rn.persistentState.Log)),
		StateMachine: make(map[string]string),
		Snapshot:     rn.persistentState.Snapshot,
		VotedFor:     rn.persistentState.VotedFor,
	}
	copy(persistentStateSnapshot.Peers, rn.persistentState.Peers)
	copy(persistentStateSnapshot.Log, rn.persistentState.Log)
	maps.Copy(persistentStateSnapshot.StateMachine, rn.persistentState.StateMachine)

	volatileStateSnapshot := rn.volatileState

	// Add log entries to log
	rn.persistentState.Log = append(rn.persistentState.Log, entries...)
	rn.releaseLockWithLogger("replicateLog 1")

	majority := rn.Majority()

	replicationTracker := util.NewReplicationTracker(majority)
	rn.logger.Debug("Replicating log entries to peers",
		slog.Int("majority", majority),
		slog.Any("entries", entries),
	)

	replicationTracker.MarkSuccess(persistentStateSnapshot.OwnPeer.ID)

	ctx, cancel := context.WithCancel(context.Background())

	// Start replication to all peers concurrently
	var wg sync.WaitGroup
	for _, peer := range persistentStateSnapshot.Peers {
		wg.Add(1)
		go func(peer util.Peer) {
			defer wg.Done()

			err := rn.replicateLogToPeer(ctx, peer, replicationTracker, persistentStateSnapshot, volatileStateSnapshot, entries)
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

	if state == util.ServerStateLeader &&
		term == persistentStateSnapshot.CurrentTerm {
		rn.updateCommitIndexAndApply()
	}

	return nil
}

// replicateLogToPeer replicates log entries to a single peer.
func (rn *RaftNode) replicateLogToPeer(
	ctx context.Context,
	peer util.Peer,
	replicationTracker *util.ReplicationTracker,
	persistentStateSnapshot util.PersistentState,
	volatileStateSnapshot util.VolatileState,
	entries []util.LogEntry,
) error {
	retryCount := 0
	targetEndIndex := len(persistentStateSnapshot.Log) + len(entries)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rn.ackquireLockWithLogger("replicateLogToPeer")

		// Check conditions for continuing
		if rn.volatileState.State != util.ServerStateLeader ||
			rn.persistentState.CurrentTerm != persistentStateSnapshot.CurrentTerm ||
			rn.firstPeerWithID(peer.ID) == nil {
			rn.releaseLockWithLogger("replicateLogToPeer")
			return nil
		}
		rn.releaseLockWithLogger("replicateLogToPeer")

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

		peerNextIndex := rn.leaderState.NextIndex[peer.ID]
		if peerNextIndex == 0 {
			peerNextIndex = len(rn.persistentState.Log) + 1
		}

		peerPrevLogIndex := peerNextIndex - 1
		peerPrevLogTerm := 0
		if peerPrevLogIndex > 0 && peerPrevLogIndex <= len(rn.persistentState.Log) {
			peerPrevLogTerm = rn.persistentState.Log[peerPrevLogIndex-1].Term
		}

		// Calculate entries to send
		var entriesToSend []util.LogEntry
		if peerNextIndex <= len(persistentStateSnapshot.Log) {
			// Peer needs entries from the original log (catch-up scenario)
			startIndex := peerNextIndex - 1 // Convert to 0-based index
			entriesToSend = rn.persistentState.Log[startIndex:]
		} else if peerNextIndex == len(persistentStateSnapshot.Log)+1 {
			// Peer is up-to-date with original log, send only new entries
			entriesToSend = entries
		} else {
			// Peer's nextIndex is beyond what we expect - reset and retry
			rn.leaderState.NextIndex[peer.ID] = len(persistentStateSnapshot.Log) + 1
			rn.releaseLockWithLogger("replicateLogToPeer 2")
			continue
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
			Term:         persistentStateSnapshot.CurrentTerm,
			LeaderID:     persistentStateSnapshot.OwnPeer.ID,
			PrevLogIndex: peerPrevLogIndex,
			PrevLogTerm:  peerPrevLogTerm,
			Entries:      entriesToSend,
			LeaderCommit: volatileStateSnapshot.CommitIndex,
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
	allMatchIndices[rn.persistentState.OwnPeer.ID] = len(rn.persistentState.Log)

	rn.mu.RUnlock()

	// Calculate new commit index based on majority match indices
	indices := make([]int, 0, len(allMatchIndices))
	for _, index := range allMatchIndices {
		indices = append(indices, index)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(indices)))

	majorityIndex := indices[rn.Majority()-1]
	rn.mu.Lock()
	defer rn.mu.Unlock()
	oldCommitIndex := rn.volatileState.CommitIndex

	// Only update commit index if it's in the current term
	for newCommitIndex := majorityIndex; newCommitIndex > oldCommitIndex; newCommitIndex-- {
		if newCommitIndex > 0 && newCommitIndex <= len(rn.persistentState.Log) {
			entry := rn.persistentState.Log[newCommitIndex-1]
			if entry.Term == rn.persistentState.CurrentTerm {
				rn.volatileState.CommitIndex = newCommitIndex
				rn.logger.Debug("Updated commit index",
					slog.Int("oldCommitIndex", oldCommitIndex),
					slog.Int("newCommitIndex", newCommitIndex))
				break
			}
		}
	}

	// Apply newly committed entries to state machine
	rn.applyCommittedEntries()
}

// Applies the committed entries to the state machine.
func (rn *RaftNode) applyCommittedEntries() {
	for rn.volatileState.LastApplied < rn.volatileState.CommitIndex {
		entry := rn.persistentState.Log[rn.volatileState.LastApplied]

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

		rn.volatileState.LastApplied++
	}
}

// Checks if the node's log is at least as up-to-date as the given log.
//
// - Parameters:
//   - lastLogIndex: The index of the last log entry.
//   - lastLogTerm: The term of the last log entry.
//
// - Returns: True if the node's log is at least as up-to-date as the given log.
func (rn *RaftNode) isLogAtLeastAsUpToDate(lastLogIndex int, lastLogTerm int) bool {
	var localLastLogTerm int
	if len(rn.persistentState.Log) > 0 {
		localLastLogTerm = rn.persistentState.Log[len(rn.persistentState.Log)-1].Term
	} else {
		localLastLogTerm = 0
	}

	if lastLogTerm != localLastLogTerm {
		return lastLogTerm > localLastLogTerm
	}

	localLastLogIndex := len(rn.persistentState.Log)
	return lastLogIndex >= localLastLogIndex
}

// -- State Changes --

// Stop leading.
func (rn *RaftNode) stopLeading() {
	rn.leaderState = util.LeaderState{
		NextIndex:  make(map[int]int),
		MatchIndex: make(map[int]int),
	}
}

// Become follower.
//
// - Parameters:
//   - newTerm: The new term.
//   - currentLeaderId: The ID of the current leader.
func (rn *RaftNode) becomeFollower(newTerm int, currentLeaderId *int) {
	rn.persistentState.CurrentTerm = newTerm
	rn.persistentState.VotedFor = nil
	rn.volatileState.State = util.ServerStateFollower
	rn.volatileState.CurrentLeaderID = currentLeaderId

	rn.stopLeading()
}

// Become candidate.
func (rn *RaftNode) becomeCandidate() {
	rn.persistentState.CurrentTerm += 1
	rn.volatileState.State = util.ServerStateCandidate
	rn.persistentState.VotedFor = &rn.persistentState.OwnPeer.ID
	rn.volatileState.CurrentLeaderID = nil

	rn.stopLeading()
}

// Become leader.
func (rn *RaftNode) becomeLeader() {
	rn.volatileState.State = util.ServerStateLeader
	rn.volatileState.CurrentLeaderID = &rn.persistentState.OwnPeer.ID

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
