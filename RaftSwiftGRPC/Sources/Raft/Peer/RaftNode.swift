import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import Logging

actor RaftNode: RaftNodeRPC {
    // MARK: - Properties

    // TODO: maybe move to persistent state
    let config: RaftConfig
    let logger: Logger

    var heartbeatTask: Task<Void, any Error>?

    var persistentState = Raft_PersistentState()
    var volatileState = Raft_VolatileState()
    var leaderState = Raft_LeaderState()

    var majority: Int {
        persistentState.peers.count / 2 + 1
    }

    init(_ ownPeer: Raft_Peer, config: RaftConfig) {
        persistentState.ownPeer = ownPeer
        self.config = config
        logger = Logger(label: "raft.RaftNode.\(ownPeer.id)")
    }

    // MARK: - Server RPCs

    func requestVote(request: Raft_RequestVoteRequest, context: ServerContext) async throws -> Raft_RequestVoteResponse {
        logger.trace("Received request vote from \(request.candidateID)")
        resetElectionTimer()

        if request.term < persistentState.currentTerm {
            return .with { response in
                response.term = persistentState.currentTerm
                response.voteGranted = false
            }
        }

        if request.term > persistentState.currentTerm {
            logger.info("Received higher term, becoming follower")
            becomeFollower(newTerm: request.term, currentLeaderId: request.candidateID)
        }

        if !persistentState.hasVotedFor || persistentState.votedFor == request.candidateID, isLogAtLeastAsUpToDate(lastLogIndex: request.lastLogIndex, lastLogTerm: request.lastLogTerm) {
            persistentState.votedFor = request.candidateID
            return .with { response in
                response.term = persistentState.currentTerm
                response.voteGranted = true
            }
        }

        return .with { response in
            response.term = persistentState.currentTerm
            response.voteGranted = false
        }
    }

    func appendEntries(request: Raft_AppendEntriesRequest, context: ServerContext) async throws -> Raft_AppendEntriesResponse {
        logger.trace("Received append entries from \(request.leaderID)")
        resetElectionTimer()

        if request.term < persistentState.currentTerm {
            return .with { response in
                response.term = persistentState.currentTerm
                response.success = false
            }
        }

        if request.term > persistentState.currentTerm {
            logger.info("Received higher term, becoming follower")
            becomeFollower(newTerm: request.term, currentLeaderId: request.leaderID)
        }

        // First, update commit index
        if request.leaderCommit > volatileState.commitIndex {
            volatileState.commitIndex = min(request.leaderCommit, UInt64(persistentState.log.count))

            applyCommittedEntries()
        }

        // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if request.prevLogIndex > 0 {
            if persistentState.log.count < request.prevLogIndex {
                logger.info("Log is too short, not matching prevLogIndex")
                return .with { response in
                    response.term = persistentState.currentTerm
                    response.success = false
                }
            }

            let prevLogTerm = persistentState.log[Int(request.prevLogIndex) - 1].term
            if prevLogTerm != request.prevLogTerm {
                // Term mismatch at the expected previous index
                logger.info("Term mismatch at the expected previous index")
                return .with { response in
                    response.term = persistentState.currentTerm
                    response.success = false
                }
            }
        }

        // If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it
        // Append any new entries not already in the log
        if request.entries.count > 0 {
            let newEntries = request.entries
            var conflictIndex: Int? = nil

            for i in 0 ..< newEntries.count {
                let entryIndex = Int(request.prevLogIndex) + i + 1

                if entryIndex <= persistentState.log.count {
                    // This is an existing entry in our log - check for conflict
                    let existingTerm = persistentState.log[entryIndex - 1].term
                    let newTerm = newEntries[i].term

                    if existingTerm != newTerm {
                        // Found a conflict - different term for same index
                        logger.info("Found a conflict - different term for same index at \(entryIndex)")
                        conflictIndex = i
                        break
                    }

                    // Entry matches, will be replicated correctly
                } else {
                    // We've reached the end of our log - remaining entries are new
                    break
                }
            }

            if let conflictIndex {
                // Remove conflicting entry and everything that follows
                let deleteFromIndex = Int(request.prevLogIndex) + conflictIndex
                persistentState.log.removeSubrange(deleteFromIndex - 1 ..< persistentState.log.count)

                // Append new entries
                persistentState.log.append(contentsOf: newEntries[conflictIndex...])
            } else {
                // No conflict - append all new entries
                let newEntriesStartIndex = max(0, persistentState.log.count - Int(request.prevLogIndex))
                if newEntriesStartIndex < newEntries.count {
                    persistentState.log.append(contentsOf: newEntries[newEntriesStartIndex...])
                }
            }
        }

        return .with { response in
            response.term = persistentState.currentTerm
            response.success = true
        }
    }

    func installSnapshot(request: Raft_InstallSnapshotRequest, context: ServerContext) async throws -> Raft_InstallSnapshotResponse {
        // TODO: implement
        .with { response in
            response.term = 0
        }
    }

    // MARK: - Node Lifecycle

    /// Starts the node.
    func start() {
        startHeartbeatTask()
    }

    /// Starts the heartbeat task.
    ///
    /// If the node is a leader, it will send heartbeats to all followers.
    /// If the node is a follower, it will check the election timeout.
    private func startHeartbeatTask() {
        // Cancel existing task if it exists
        heartbeatTask?.cancel()

        let task = Task {
            while !Task.isCancelled {
                if volatileState.state == .leader {
                    try await self.sendHeartbeat()
                } else {
                    try await self.checkElectionTimeout()
                }
                try await Task.sleep(for: .milliseconds(config.heartbeatInterval))
            }
        }

        heartbeatTask = task
    }

    /// Sends a heartbeat to all followers.
    private func sendHeartbeat() async throws {
        guard volatileState.state == .leader else {
            throw RaftError.notLeader
        }

        logger.trace("Sending heartbeat to followers")
        try await replicateLog(entries: [])
    }

    // MARK: - Election

    /// Resets the election timer.
    private func resetElectionTimer() {
        volatileState.lastHeartbeat = .now()
    }

    /// Checks if the election timeout has been reached.
    private func checkElectionTimeout() async throws {
        let now = Date()
        if now.timeIntervalSince(volatileState.lastHeartbeat.date) * 1000 > Double(volatileState.electionTimeout) {
            logger.info("Election timeout reached, becoming candidate")
            try await startElection()
        }
    }

    /// Starts an election.
    private func startElection() async throws {
        logger.trace("Starting election")
        becomeCandidate()

        // Reset election timeout
        volatileState.electionTimeout = UInt32.random(in: config.electionTimeoutRange)
        resetElectionTimer()

        // Request votes from other nodes
        try await requestVotes()
    }

    /// Requests votes from all peers.
    private func requestVotes() async throws {
        logger.trace("Requesting votes from peers: \(persistentState.peers)")

        // Count own vote
        var votes = 1
        let requiredVotes = majority

        let persistentStateSnapshot = persistentState

        try await withThrowingTaskGroup { group in
            for peer in persistentState.peers {
                group.addTask {
                    self.logger.trace("Requesting vote from \(peer.id)")
                    // TODO: helper method
                    return try await self.withClient(peer: peer) { client in
                        let result = try await client.requestVote(.with { voteRequest in
                            voteRequest.term = persistentStateSnapshot.currentTerm
                            voteRequest.candidateID = persistentStateSnapshot.ownPeer.id
                            voteRequest.lastLogIndex = UInt64(persistentStateSnapshot.log.count)
                            voteRequest.lastLogTerm = persistentStateSnapshot.log.last?.term ?? 0
                        })

                        return (peer.id, result)
                    }
                }
            }

            for try await (peerId, vote) in group {
                if Task.isCancelled {
                    break
                }

                logger.trace("Received vote from \(peerId): \(vote.voteGranted)")

                // Check if the peer has a higher term
                if vote.term > persistentState.currentTerm {
                    logger.info("Received higher term, becoming follower")
                    becomeFollower(newTerm: vote.term, currentLeaderId: peerId)
                    return
                }

                // Count votes only if we're still a candidate and in the same term
                if volatileState.state == .candidate, vote.term == persistentState.currentTerm, vote.voteGranted {
                    votes += 1

                    if votes >= requiredVotes {
                        logger.info("Received majority of votes (\(votes) / \(persistentState.peers.count)), becoming leader")
                        becomeLeader()

                        // Cancel remaining tasks
                        group.cancelAll()
                        return
                    }
                }
            }

            if volatileState.state == .candidate {
                logger.info("Election failed, received votes: \(votes) / \(persistentState.peers.count)")
            }
        }
    }

    // MARK: - Log Replication

    /// Replicates log entries to all peers.
    ///
    /// - Parameter entries: The log entries to replicate.
    private func replicateLog(entries: [Raft_LogEntry]) async throws {
        guard volatileState.state == .leader else {
            logger.info("Not a leader, not replicating log")
            throw RaftError.notLeader
        }

        resetElectionTimer()

        let persistentStateSnapshot = persistentState
        let volatileStateSnapshot = volatileState

        // Add log entries to log
        persistentState.log.append(contentsOf: entries)

        // Create replication tracker with leader pre-marked as successful
        let replicationTracker = ReplicationTracker<UInt32>(majority: majority)
        await replicationTracker.markSuccess(id: persistentStateSnapshot.ownPeer.id)

        // Start a background task for replication
        Task {
            await withThrowingTaskGroup { group in
                // Start individual replication tasks for each peer
                for peer in persistentStateSnapshot.peers {
                    group.addTask {
                        try await self.replicateLogToPeer(
                            peer: peer,
                            replicationTracker: replicationTracker,
                            persistentStateSnapshot: persistentStateSnapshot,
                            volatileStateSnapshot: volatileStateSnapshot,
                            entries: entries
                        )
                    }
                }
            }

            // Wait for majority of peers to have replicated the log
            await replicationTracker.waitForMajority()

            // Once majority has replicated the log, update commit index
            // and apply committed entries
            if volatileState.state == .leader, persistentState.currentTerm == persistentStateSnapshot.currentTerm {
                await updateCommitIndexAndApply(entries: entries, prevLogIndexSnapshot: persistentStateSnapshot.log.count)
            }
        }

        // Wait for majority to have replicated the log before returning
        await replicationTracker.waitForMajority()
    }

    /// Replicates log entries to a single peer.
    ///
    /// - Parameters:
    ///   - peer: The peer to replicate the log to.
    ///   - replicationTracker: The replication tracker.
    ///   - persistentStateSnapshot: The persistent state snapshot.
    ///   - volatileStateSnapshot: The volatile state snapshot.
    ///   - entries: The log entries to replicate.
    private func replicateLogToPeer(
        peer: Raft_Peer,
        replicationTracker: ReplicationTracker<UInt32>,
        persistentStateSnapshot: Raft_PersistentState,
        volatileStateSnapshot: Raft_VolatileState,
        entries: [Raft_LogEntry]
    ) async throws {
        var retryCount = 0

        // Continue trying until successful or no longer leader
        while !Task.isCancelled, volatileState.state == .leader, persistentState.currentTerm == persistentStateSnapshot.currentTerm, persistentState.peers.contains(peer) {
            // Check if already successful (another task marked it as successful)
            if await replicationTracker.isSuccessful(id: peer.id) {
                return
            }

            do {
                let peerNextIndex = leaderState.nextIndex[peer.id] ?? UInt64(persistentStateSnapshot.log.count) + 1
                let peerPrevLogIndex = peerNextIndex - 1
                let peerPrevLogTerm = if peerPrevLogIndex <= 0 {
                    UInt64(0)
                } else {
                    persistentStateSnapshot.log[Int(peerPrevLogIndex)].term
                }

                // Calculate entries to send
                let entriesToSend: [Raft_LogEntry]
                if peerNextIndex <= UInt64(persistentStateSnapshot.log.count) {
                    // Need to send some previous entries
                    let startIndex = max(0, Int(peerNextIndex) - 1)
                    entriesToSend = Array(persistentStateSnapshot.log[startIndex...])
                } else {
                    entriesToSend = entries
                }

                logger.trace("Sending append entries to \(peer.id) with nextIndex: \(peerNextIndex), prevLogIndex: \(peerPrevLogIndex), prevLogTerm: \(peerPrevLogTerm), entries.count: \(entriesToSend.count)")

                let result = try await withClient(peer: peer) { client in
                    try await client.appendEntries(.with { appendEntriesRequest in
                        appendEntriesRequest.term = persistentStateSnapshot.currentTerm
                        appendEntriesRequest.leaderID = persistentStateSnapshot.ownPeer.id
                        appendEntriesRequest.prevLogIndex = peerPrevLogIndex
                        appendEntriesRequest.prevLogTerm = peerPrevLogTerm
                        appendEntriesRequest.entries = entriesToSend
                        appendEntriesRequest.leaderCommit = volatileStateSnapshot.commitIndex
                    })
                }

                if Task.isCancelled {
                    return
                }

                if result.term > persistentState.currentTerm {
                    logger.info("Received higher term, becoming follower")
                    becomeFollower(newTerm: result.term, currentLeaderId: peer.id)
                    return
                }

                if result.success {
                    await replicationTracker.markSuccess(id: peer.id)

                    let newMatchIndex = peerPrevLogIndex + UInt64(entriesToSend.count)
                    leaderState.matchIndex[peer.id] = newMatchIndex
                    leaderState.nextIndex[peer.id] = newMatchIndex + 1

                    return
                } else {
                    // Log inconsistency, decrement nextIndex and retry
                    leaderState.nextIndex[peer.id] = max(1, (leaderState.nextIndex[peer.id] ?? 1) - 1)
                    retryCount += 1
                    logger.info("Append entries failed for \(peer.id), retrying with earlier index, retrying with index \(leaderState.nextIndex[peer.id] ?? 0)")

                    // Wait a bit before retrying with exponential backoff
                    try await Task.sleep(for: .milliseconds(100 * UInt64(min(64, 1 << retryCount))))
                }
            } catch {
                logger.error("Failed to replicate log to \(peer.id): \(error)")
                retryCount += 1

                // Wait a bit before retrying with exponential backoff
                try await Task.sleep(for: .milliseconds(100 * UInt64(min(64, 1 << retryCount))))
            }
        }
    }

    /// Executes a block of code with a gRPC client for a specific peer.
    ///
    /// - Parameters:
    ///   - peer: The peer to execute the block for.
    ///   - body: The block to execute.
    /// - Throws: Any errors thrown by the block.
    /// - Returns: The result of the block.
    private func withClient<T: Sendable>(peer: Raft_Peer, _ body: @Sendable @escaping (_ client: Raft_RaftPeer.Client<HTTP2ClientTransport.Posix>) async throws -> T) async throws -> T {
        try await withGRPCClient(
            transport: .http2NIOPosix(
                // TODO: handel domains
                target: .ipv4(
                    host: peer.address,
                    port: Int(peer.port)
                ),
                transportSecurity: .plaintext
            ),
        ) { client in
            let peerClient = Raft_RaftPeer.Client(wrapping: client)
            return try await body(peerClient)
        }
    }

    /// Updates the commit index and applies the committed entries to the state machine.
    ///
    /// - Parameters:
    ///   - entries: The log entries to apply.
    ///   - prevLogIndexSnapshot: The previous log index snapshot.
    private func updateCommitIndexAndApply(
        entries _: [Raft_LogEntry],
        prevLogIndexSnapshot _: Int
    ) async {
        // Add own match index (implicitly the end of the log)
        var allMatchIndices = leaderState.matchIndex
        allMatchIndices[persistentState.ownPeer.id] = UInt64(persistentState.log.count)

        // Calculate new commit index based on majority match indices
        let sortedIndices = Array(allMatchIndices.values).sorted()
        let majorityIndex = sortedIndices[majority - 1]

        // Only update commit index if it's in the current term
        // (Raft safety requirement: only commit entries from current term)
        if volatileState.commitIndex < majorityIndex {
            for i in stride(from: majorityIndex, through: volatileState.commitIndex + 1, by: -1) {
                if i <= persistentState.log.count, persistentState.log[Int(i - 1)].term == persistentState.currentTerm {
                    volatileState.commitIndex = i
                    // Break the loop as soon as the term matches, since go
                    break
                }
            }
        }

        // Apply newly committed entries to state machine
        applyCommittedEntries()
    }

    /// Applies the committed entries to the state machine.
    private func applyCommittedEntries() {
        while volatileState.lastApplied < volatileState.commitIndex {
            let entry = persistentState.log[Int(volatileState.lastApplied)]
            persistentState.stateMachine[entry.key] = entry.value
            volatileState.lastApplied += 1
        }
    }

    /// Checks if the log is at least as up to date as the given log.
    ///
    /// - Parameters:
    ///   - lastLogIndex: The index of other node's last log entry.
    ///   - lastLogTerm: The term of other node's last log entry.
    private func isLogAtLeastAsUpToDate(lastLogIndex: UInt64, lastLogTerm: UInt64) -> Bool {
        let localLastLogTerm = persistentState.log.last?.term ?? 0
        let localLastLogIndex = persistentState.log.count

        if lastLogTerm != localLastLogTerm {
            return lastLogTerm > localLastLogTerm
        }

        return lastLogIndex >= localLastLogIndex
    }

    // MARK: - State Changes

    /// Let the node stop leading.
    private func stopLeading() {
        if let heartbeatTask {
            heartbeatTask.cancel()
            self.heartbeatTask = nil
        }

        leaderState = .init()
    }

    /// Let the node become a follower.
    ///
    /// - Parameters:
    ///   - newTerm: The new term.
    ///   - currentLeaderId: The ID of the current leader.
    private func becomeFollower(newTerm: UInt64, currentLeaderId: UInt32) {
        persistentState.currentTerm = newTerm
        persistentState.clearVotedFor()
        volatileState.state = .follower
        volatileState.currentLeaderID = currentLeaderId

        stopLeading()
    }

    /// Let the node become a candidate.
    private func becomeCandidate() {
        persistentState.currentTerm += 1
        volatileState.state = .candidate
        persistentState.votedFor = persistentState.ownPeer.id
        volatileState.clearCurrentLeaderID()

        stopLeading()
    }

    /// Let the node become a leader.
    private func becomeLeader() {
        volatileState.state = .leader
        volatileState.currentLeaderID = persistentState.ownPeer.id

        for peer in persistentState.peers {
            leaderState.nextIndex[peer.id] = UInt64(persistentState.log.count + 1)
            leaderState.matchIndex[peer.id] = 0
        }
    }
}
