import Foundation
import Logging

public actor RaftNode {
    // MARK: - Properties

    private let transport: any RaftPeerTransport
    let logger: Logger

    var heartbeatTask: Task<Void, any Error>?

    var persistentState: PersistentState
    var volatileState: VolatileState
    var leaderState: LeaderState

    var majority: Int {
        (persistentState.peers.count + 1) / 2 + 1
    }

    // MARK: - Init

    public init(_ ownPeer: Peer, peers: [Peer], config: RaftConfig, transport: any RaftPeerTransport) {
        self.transport = transport
        logger = Logger(label: "raft.RaftNode.\(ownPeer.id)")

        persistentState = PersistentState(
            ownPeer: ownPeer,
            peers: peers,
            config: config
        )
        volatileState = VolatileState()
        leaderState = LeaderState()
    }

    // MARK: - Server RPCs

    public func requestVote(request: RequestVoteRequest) async throws -> RequestVoteResponse {
        logger.trace("Received request vote from \(request.candidateID)")
        resetElectionTimer()

        if request.term < persistentState.currentTerm {
            return RequestVoteResponse(
                term: persistentState.currentTerm,
                voteGranted: false
            )
        }

        if request.term > persistentState.currentTerm {
            logger.info("Received higher term, becoming follower")

            becomeFollower(newTerm: request.term, currentLeaderId: request.candidateID)
        }

        if persistentState.votedFor == nil || persistentState.votedFor == request.candidateID, isLogAtLeastAsUpToDate(lastLogIndex: request.lastLogIndex, lastLogTerm: request.lastLogTerm) {
            persistentState.votedFor = request.candidateID

            return RequestVoteResponse(
                term: persistentState.currentTerm,
                voteGranted: true
            )
        }

        return RequestVoteResponse(
            term: persistentState.currentTerm,
            voteGranted: false
        )
    }

    public func appendEntries(request: AppendEntriesRequest) async throws -> AppendEntriesResponse {
        logger.trace("Received append entries from \(request.leaderID)")
        resetElectionTimer()

        if request.term < persistentState.currentTerm {
            return AppendEntriesResponse(
                term: persistentState.currentTerm,
                success: false
            )
        }

        if request.term > persistentState.currentTerm {
            logger.info("Received higher term, becoming follower")

            becomeFollower(newTerm: request.term, currentLeaderId: request.leaderID)
        }

        // First, update commit index
        if request.leaderCommit > volatileState.commitIndex {
            volatileState.commitIndex = min(request.leaderCommit, persistentState.log.count)

            applyCommittedEntries()
        }

        // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if request.prevLogIndex > 0 {
            if persistentState.log.count < request.prevLogIndex {
                logger.info("Log is too short, not matching prevLogIndex")

                return AppendEntriesResponse(
                    term: persistentState.currentTerm,
                    success: false
                )
            }

            let prevLogTerm = persistentState.log[request.prevLogIndex - 1].term
            if prevLogTerm != request.prevLogTerm {
                // Term mismatch at the expected previous index
                logger.info("Term mismatch at the expected previous index")

                return AppendEntriesResponse(
                    term: persistentState.currentTerm,
                    success: false
                )
            }
        }

        // If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it
        // Append any new entries not already in the log
        if request.entries.count > 0 {
            let newEntries = request.entries
            var conflictIndex: Int? = nil

            for i in 0 ..< newEntries.count {
                let entryIndex = request.prevLogIndex + i + 1

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
                let deleteFromIndex = request.prevLogIndex + conflictIndex
                persistentState.log.removeSubrange(deleteFromIndex - 1 ..< persistentState.log.count)

                // Append new entries
                persistentState.log.append(contentsOf: newEntries[conflictIndex...])
            } else {
                // No conflict - append all new entries
                let newEntriesStartIndex = max(0, persistentState.log.count - request.prevLogIndex)
                if newEntriesStartIndex < newEntries.count {
                    persistentState.log.append(contentsOf: newEntries[newEntriesStartIndex...])
                }
            }
        }

        return AppendEntriesResponse(
            term: persistentState.currentTerm,
            success: true
        )
    }

    // MARK: - Client RPCs

    public func put(request: PutRequest) async throws -> PutResponse {
        guard volatileState.state == .leader else {
            return PutResponse(success: false, leaderHint: persistentState.peers.first { $0.id == volatileState.currentLeaderID })
        }

        try await replicateLog(entries: [LogEntry(term: persistentState.currentTerm, key: request.key, value: request.value)])

        return PutResponse(success: true)
    }

    public func get(request: GetRequest) async throws -> GetResponse {
        guard volatileState.state == .leader else {
            return GetResponse(leaderHint: persistentState.peers.first { $0.id == volatileState.currentLeaderID })
        }

        return GetResponse(value: persistentState.stateMachine[request.key])
    }

    public func getDebug(request: GetRequest) async throws -> GetResponse {
        GetResponse(value: persistentState.stateMachine[request.key])
    }

    public func getState() async throws -> ServerStateResponse {
        ServerStateResponse(
            id: persistentState.ownPeer.id,
            state: volatileState.state
        )
    }

    public func getTerm() async throws -> ServerTermResponse {
        ServerTermResponse(
            id: persistentState.ownPeer.id,
            term: persistentState.currentTerm
        )
    }

    // MARK: - Node Lifecycle

    /// Starts the node.
    public func start() {
        startHeartbeatTask()
    }

    /// Shuts down the node.
    public func shutdown() {
        heartbeatTask?.cancel()
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
                    try await Task.sleep(for: .milliseconds(persistentState.config.heartbeatInterval))
                } else {
                    try await self.checkElectionTimeout()
                    try await Task.sleep(for: .milliseconds(persistentState.config.heartbeatInterval * 3))
                }
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
        volatileState.lastHeartbeat = Date()
    }

    /// Checks if the election timeout has been reached.
    private func checkElectionTimeout() async throws {
        let now = Date()
        if now.timeIntervalSince(volatileState.lastHeartbeat) * 1000 > Double(volatileState.electionTimeout) {
            logger.info("Election timeout reached, becoming candidate")
            try await startElection()
        }
    }

    /// Starts an election.
    private func startElection() async throws {
        logger.trace("Starting election")
        becomeCandidate()

        // Reset election timeout
        volatileState.electionTimeout = Int.random(in: persistentState.config.electionTimeoutRange)
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

        await withTaskGroup { group in
            for peer in persistentState.peers {
                group.addTask {
                    await self.requestVoteFromPeer(
                        peer: peer,
                        persistentStateSnapshot: persistentStateSnapshot
                    )
                }
            }

            for await (peerId, vote) in group {
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
                        logger.info("Received majority of votes (\(votes) / \(persistentState.peers.count + 1)), becoming leader")
                        becomeLeader()

                        // Cancel remaining tasks
                        group.cancelAll()
                        return
                    }
                }
            }

            if volatileState.state == .candidate {
                logger.info("Election failed, received votes: \(votes) / \(persistentState.peers.count + 1)")
            }
        }
    }

    func requestVoteFromPeer(
        peer: Peer,
        persistentStateSnapshot: PersistentState
    ) async -> (Int, RequestVoteResponse) {
        logger.trace("Requesting vote from \(peer.id)")
        do {
            let voteRequest = RequestVoteRequest(
                term: persistentStateSnapshot.currentTerm,
                candidateID: persistentStateSnapshot.ownPeer.id,
                lastLogIndex: persistentStateSnapshot.log.count,
                lastLogTerm: persistentStateSnapshot.log.last?.term ?? 0
            )
            let response = try await transport.requestVote(
                voteRequest,
                to: peer,
                isolation: #isolation
            )
            return (peer.id, response)
        } catch {
            logger.error("Failed to request vote from peer \(peer.id): \(error)")
            return (peer.id, RequestVoteResponse(term: 0, voteGranted: false))
        }
    }

    // MARK: - Log Replication

    /// Replicates log entries to all peers.
    ///
    /// - Parameter entries: The log entries to replicate.
    private func replicateLog(entries: [LogEntry]) async throws {
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
        let replicationTracker = ReplicationTracker(majority: majority)
        logger.trace("Replicating log entries to peers", metadata: [
            "majority": .stringConvertible(majority),
            "entries": .stringConvertible(entries),
        ])
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

                // Wait for majority of peers to have replicated the log
                await replicationTracker.waitForMajority()
                logger.trace("Majority of peers have replicated the log", metadata: [
                    "messageHash": .stringConvertible(entries.hashValue),
                ])

                // Once majority has replicated the log, update commit index
                // and apply committed entries
                if volatileState.state == .leader, persistentState.currentTerm == persistentStateSnapshot.currentTerm {
                    await updateCommitIndexAndApply()
                }
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
        peer: Peer,
        replicationTracker: ReplicationTracker,
        persistentStateSnapshot: PersistentState,
        volatileStateSnapshot: VolatileState,
        entries: [LogEntry]
    ) async throws {
        var retryCount = 0

        // Continue trying until successful or no longer leader
        while !Task.isCancelled, volatileState.state == .leader, persistentState.currentTerm == persistentStateSnapshot.currentTerm, persistentState.peers.contains(peer) {
            // Check if already successful (another task marked it as successful)
            if await replicationTracker.isSuccessful(id: peer.id) {
                return
            }

            do {
                let peerNextIndex = leaderState.nextIndex[peer.id] ?? persistentStateSnapshot.log.count + 1
                let peerPrevLogIndex = peerNextIndex - 1
                let peerPrevLogTerm = if peerPrevLogIndex > 0, peerPrevLogIndex <= persistentStateSnapshot.log.count {
                    persistentStateSnapshot.log[peerPrevLogIndex - 1].term
                } else {
                    0
                }

                // Calculate entries to send
                let entriesToSend: [LogEntry]
                if peerNextIndex <= persistentStateSnapshot.log.count {
                    // Need to send some previous entries
                    let startIndex = max(0, peerNextIndex - 1)
                    entriesToSend = Array(persistentStateSnapshot.log[startIndex...])
                } else {
                    entriesToSend = entries
                }

                logger.trace("Sending append entries to \(peer.id) with nextIndex: \(peerNextIndex), prevLogIndex: \(peerPrevLogIndex), prevLogTerm: \(peerPrevLogTerm), entries.count: \(entriesToSend.count)")

                let appendEntriesRequest = AppendEntriesRequest(
                    term: persistentStateSnapshot.currentTerm,
                    leaderID: persistentStateSnapshot.ownPeer.id,
                    prevLogIndex: peerPrevLogIndex,
                    prevLogTerm: peerPrevLogTerm,
                    entries: entriesToSend,
                    leaderCommit: volatileStateSnapshot.commitIndex
                )

                let result = try await transport.appendEntries(appendEntriesRequest, to: peer, isolation: #isolation)

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
                    logger.trace("Append entries successful for \(peer.id)", metadata: [
                        "messageHash": .stringConvertible(entriesToSend.hashValue),
                        "nextIndex": .stringConvertible(leaderState.nextIndex[peer.id] ?? 0),
                        "matchIndex": .stringConvertible(leaderState.matchIndex[peer.id] ?? 0),
                    ])

                    let newMatchIndex = peerPrevLogIndex + entriesToSend.count
                    leaderState.matchIndex[peer.id] = newMatchIndex
                    leaderState.nextIndex[peer.id] = newMatchIndex + 1

                    return
                } else {
                    // Log inconsistency, decrement nextIndex and retry
                    leaderState.nextIndex[peer.id] = max(1, (leaderState.nextIndex[peer.id] ?? 1) - 1)
                    retryCount += 1
                    logger.info("Append entries failed for \(peer.id), retrying with earlier index, retrying with index \(leaderState.nextIndex[peer.id] ?? 0)", metadata: [
                        "messageHash": .stringConvertible(entries.hashValue),
                        "retryCount": .stringConvertible(retryCount),
                        "nextIndex": .stringConvertible(leaderState.nextIndex[peer.id] ?? 0),
                    ])

                    // Wait a bit before retrying with exponential backoff
                    try await Task.sleep(for: .milliseconds(100 * min(64, 1 << retryCount)))
                }
            } catch {
                let errorMessage: Logger.Message = "Failed to replicate log to \(peer.id): \(error)"
                let metadata: Logger.Metadata = [
                    "messageHash": .stringConvertible(entries.hashValue),
                    "retryCount": .stringConvertible(retryCount),
                ]
                if retryCount == 0 {
                    logger.error(errorMessage, metadata: metadata)
                } else {
                    logger.trace(errorMessage, metadata: metadata)
                }
                retryCount += 1

                // Wait a bit before retrying with exponential backoff
                try await Task.sleep(for: .milliseconds(100 * min(64, 1 << retryCount)))
            }
        }
    }

    /// Updates the commit index and applies the committed entries to the state machine.
    private func updateCommitIndexAndApply() async {
        // Add own match index (implicitly the end of the log)
        var allMatchIndices = leaderState.matchIndex
        allMatchIndices[persistentState.ownPeer.id] = persistentState.log.count

        // Calculate new commit index based on majority match indices
        let sortedIndices = Array(allMatchIndices.values).sorted()
        let majorityIndex = sortedIndices[majority - 1]

        // Only update commit index if it's in the current term
        // (Raft safety requirement: only commit entries from current term)
        if volatileState.commitIndex < majorityIndex {
            for i in stride(from: majorityIndex, through: volatileState.commitIndex + 1, by: -1) {
                if i <= persistentState.log.count, persistentState.log[i - 1].term == persistentState.currentTerm {
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
            let entry = persistentState.log[volatileState.lastApplied]

            if let key = entry.key {
                persistentState.stateMachine[key] = entry.value
            }

            volatileState.lastApplied += 1
        }
    }

    /// Checks if the log is at least as up to date as the given log.
    ///
    /// - Parameters:
    ///   - lastLogIndex: The index of other node's last log entry.
    ///   - lastLogTerm: The term of other node's last log entry.
    private func isLogAtLeastAsUpToDate(lastLogIndex: Int, lastLogTerm: Int) -> Bool {
        let localLastLogTerm = persistentState.log.last?.term ?? 0

        if lastLogTerm != localLastLogTerm {
            return lastLogTerm > localLastLogTerm
        }

        let localLastLogIndex = persistentState.log.count
        return lastLogIndex >= localLastLogIndex
    }

    // MARK: - State Changes

    /// Let the node stop leading.
    private func stopLeading() {
        leaderState = .init()
    }

    /// Let the node become a follower.
    ///
    /// - Parameters:
    ///   - newTerm: The new term.
    ///   - currentLeaderId: The ID of the current leader.
    private func becomeFollower(newTerm: Int, currentLeaderId: Int) {
        persistentState.currentTerm = newTerm
        persistentState.votedFor = nil
        volatileState.state = .follower
        volatileState.currentLeaderID = currentLeaderId

        stopLeading()
    }

    /// Let the node become a candidate.
    private func becomeCandidate() {
        persistentState.currentTerm += 1
        volatileState.state = .candidate
        persistentState.votedFor = persistentState.ownPeer.id
        volatileState.currentLeaderID = nil

        stopLeading()
    }

    /// Let the node become a leader.
    private func becomeLeader() {
        volatileState.state = .leader
        volatileState.currentLeaderID = persistentState.ownPeer.id

        for peer in persistentState.peers {
            leaderState.nextIndex[peer.id] = persistentState.log.count + 1
            leaderState.matchIndex[peer.id] = 0
        }
    }
}
