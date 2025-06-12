import Foundation
import Logging

public actor RaftNode {
    // MARK: - Properties

    /// The transport layer for peer-to-peer communication.
    private let transport: any RaftPeerTransport
    /// The persistence layer to save snapshots
    private let persistence: any RaftNodePersistence
    private var isSnapshotting: Bool = false
    /// The logger for logging messages.
    let logger: Logger

    /// The task for the heartbeat/election loop.
    private var heartbeatTask: Task<Void, Never>?

    /// The persistent state of the node.
    var persistentState: PersistentState
    /// The volatile state of the node.
    var volatileState: VolatileState
    /// The leader state of the node.
    var leaderState: LeaderState

    /// The number of votes needed to become the leader or commit a log entry.
    var majority: Int {
        (persistentState.peers.count + 1) / 2 + 1
    }

    // MARK: - Init

    /// Initializes a new instance of the RaftNode class.
    ///
    /// - Parameters:
    ///   - ownPeer: The own peer.
    ///   - peers: The list of peers.
    ///   - config: The configuration.
    ///   - transport: The transport layer.
    public init(_ ownPeer: Peer, peers: [Peer], config: RaftConfig, transport: any RaftPeerTransport, persistence: any RaftNodePersistence) {
        self.transport = transport
        self.persistence = persistence
        logger = Logger(label: "raft.RaftNode.\(ownPeer.id)")

        persistentState = PersistentState(
            ownPeer: ownPeer,
            peers: peers,
            config: config,
        )
        volatileState = VolatileState()
        leaderState = LeaderState()
    }

    // MARK: - Server RPCs

    /// Handles a RequestVote RPC.
    ///
    /// - Parameters:
    ///   - request: The RequestVoteRequest to handle.
    /// - Returns: The RequestVoteResponse.
    public func requestVote(request: RequestVoteRequest) async -> RequestVoteResponse {
        logger.trace("Received request vote from \(request.candidateID)", metadata: [
            "term": .stringConvertible(request.term),
            "myTerm": .stringConvertible(persistentState.currentTerm),
        ])
        resetElectionTimer()

        if request.term < persistentState.currentTerm {
            logger.info("Received lower term \(request.term), not voting for \(request.candidateID)")
            return RequestVoteResponse(
                term: persistentState.currentTerm,
                voteGranted: false,
            )
        } else if request.term > persistentState.currentTerm {
            logger.info("Received higher term \(request.term), becoming follower of \(request.candidateID)")

            becomeFollower(newTerm: request.term, currentLeaderId: request.candidateID)
        }

        if persistentState.votedFor == nil || persistentState.votedFor == request.candidateID, isLogAtLeastAsUpToDate(lastLogIndex: request.lastLogIndex, lastLogTerm: request.lastLogTerm) {
            logger.trace("Granting vote to \(request.candidateID)")
            persistentState.votedFor = request.candidateID

            return RequestVoteResponse(
                term: persistentState.currentTerm,
                voteGranted: true,
            )
        }

        return RequestVoteResponse(
            term: persistentState.currentTerm,
            voteGranted: false,
        )
    }

    /// Handles an AppendEntries RPC.
    ///
    /// - Parameters:
    ///   - request: The AppendEntriesRequest to handle.
    /// - Returns: The AppendEntriesResponse.
    public func appendEntries(request: AppendEntriesRequest) async -> AppendEntriesResponse {
        logger.trace("Received append entries from \(request.leaderID)")
        resetElectionTimer()

        if request.term < persistentState.currentTerm {
            return AppendEntriesResponse(
                term: persistentState.currentTerm,
                success: false,
            )
        } else if request.term > persistentState.currentTerm {
            logger.info("Received higher term \(request.term), becoming follower of \(request.leaderID)")
            becomeFollower(newTerm: request.term, currentLeaderId: request.leaderID)
        } else if volatileState.state == .candidate {
            // Own term and term of leader are the same
            // If the node is a candidate, it should become a follower
            becomeFollower(newTerm: persistentState.currentTerm, currentLeaderId: request.leaderID)
        } else if volatileState.currentLeaderID != request.leaderID {
            logger.info("Received append entries from a different leader, becoming follower of \(request.leaderID)")
            becomeFollower(newTerm: persistentState.currentTerm, currentLeaderId: request.leaderID)
        }

        // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if request.prevLogIndex > 0 {
            if persistentState.log.count < request.prevLogIndex {
                logger.info("Log is too short (length: \(persistentState.log.count), needed: \(request.prevLogIndex))")

                return AppendEntriesResponse(
                    term: persistentState.currentTerm,
                    success: false,
                )
            }

            let prevLogTerm = persistentState.log[request.prevLogIndex - 1].term
            if prevLogTerm != request.prevLogTerm {
                // Term mismatch at the expected previous index
                logger.info("Term mismatch at prevLogIndex \(request.prevLogIndex): expected \(request.prevLogTerm), got \(prevLogTerm)")

                return AppendEntriesResponse(
                    term: persistentState.currentTerm,
                    success: false,
                )
            }
        }

        // If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it
        if !request.entries.isEmpty {
            var conflictIndex: Int? = nil

            for (i, newEntry) in request.entries.enumerated() {
                let logIndex = request.prevLogIndex + i + 1
                let arrayIndex = logIndex - 1

                if arrayIndex < persistentState.log.count {
                    // This is an existing entry in our log - check for conflict
                    let existingEntry = persistentState.log[arrayIndex]
                    if existingEntry.term != newEntry.term {
                        // Found a conflict - different term for same index
                        logger.info("Found conflict at index \(logIndex): existing term \(existingEntry.term), new term \(newEntry.term)")
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
                let deleteFromIndex = request.prevLogIndex + conflictIndex + 1
                let deleteFromArrayIndex = deleteFromIndex - 1

                logger.info("Truncating log from index \(deleteFromIndex)")
                persistentState.log.removeSubrange(deleteFromArrayIndex ..< persistentState.log.count)
            }

            // Append any new entries not already in the log
            let startAppendIndex = max(0, persistentState.log.count - request.prevLogIndex)
            if startAppendIndex < request.entries.count {
                let entriesToAppend = request.entries[startAppendIndex...]
                logger.trace("Appending \(entriesToAppend.count) entries starting from log index \(persistentState.log.count + 1)")
                persistentState.log.append(contentsOf: entriesToAppend)
            }
        }

        // Update commit index
        if request.leaderCommit > volatileState.commitIndex {
            let lastLogIndex = persistentState.log.count
            volatileState.commitIndex = min(request.leaderCommit, lastLogIndex)
            logger.trace("Updating commit index to \(volatileState.commitIndex)")

            applyCommittedEntries()
        }

        return AppendEntriesResponse(
            term: persistentState.currentTerm,
            success: true,
        )
    }

    public func installSnapshot(request: InstallSnapshotRequest) async throws -> InstallSnapshotResponse {
        logger.trace("Received install snapshot from \(request.leaderID)")
        resetElectionTimer()

        // Reply immediately if term < currentTerm
        if request.term < persistentState.currentTerm {
            return InstallSnapshotResponse(term: persistentState.currentTerm)
        }

        // Update term and become follower if necessary
        if request.term > persistentState.currentTerm {
            logger.info("Received higher term \(request.term), becoming follower of \(request.leaderID)")
            becomeFollower(newTerm: request.term, currentLeaderId: request.leaderID)
        }

        // Save snapshot
        try await persistence.saveSnapshot(request.snapshot, for: persistentState.ownPeer.id)
        persistentState.snapshot = request.snapshot

        let snapshotLastIndex = request.snapshot.lastIncludedIndex
        let snapshotLastTerm = request.snapshot.lastIncludedTerm

        if persistentState.log.count >= snapshotLastIndex {
            let logIndex = snapshotLastIndex - 1
            if logIndex >= 0, persistentState.log[logIndex].term == snapshotLastTerm {
                // Keep log entries after the snapshot
                persistentState.log.removeSubrange(0 ..< snapshotLastIndex)
                logger.info("Kept \(persistentState.log.count) log entries after installing snapshot")
            } else {
                // Discard entire log - conflict detected
                persistentState.log = []
                logger.info("Discarded entire log due to conflict with snapshot")
            }
        } else {
            // Log is shorter than snapshot - discard entire log
            persistentState.log = []
            logger.info("Discarded entire log - shorter than snapshot")
        }

        // Reset state machine using snapshot
        persistentState.stateMachine = request.snapshot.stateMachine

        // Update commit and last applied indices
        volatileState.commitIndex = max(volatileState.commitIndex, snapshotLastIndex)
        volatileState.lastApplied = max(volatileState.lastApplied, snapshotLastIndex)

        logger.info("Successfully installed snapshot up to index \(snapshotLastIndex)")
        return InstallSnapshotResponse(term: persistentState.currentTerm)
    }

    // MARK: - Client RPCs

    /// Handles a Put RPC.
    ///
    /// - Parameters:
    ///   - request: The PutRequest to handle.
    /// - Returns: The PutResponse.
    public func put(request: PutRequest) async throws -> PutResponse {
        guard volatileState.state == .leader else {
            return PutResponse(success: false, leaderHint: persistentState.peers.first { $0.id == volatileState.currentLeaderID })
        }

        try await replicateLog(entries: [LogEntry(term: persistentState.currentTerm, key: request.key, value: request.value)])

        return PutResponse(success: true)
    }

    /// Handles a Get RPC.
    ///
    /// - Parameters:
    ///   - request: The GetRequest to handle.
    /// - Returns: The GetResponse.
    public func get(request: GetRequest) async -> GetResponse {
        guard volatileState.state == .leader else {
            return GetResponse(leaderHint: persistentState.peers.first { $0.id == volatileState.currentLeaderID })
        }

        return GetResponse(value: persistentState.stateMachine[request.key])
    }

    /// Handles a GetDebug RPC.
    ///
    /// - Parameters:
    ///   - request: The GetRequest to handle.
    /// - Returns: The GetResponse.
    public func getDebug(request: GetRequest) async -> GetResponse {
        GetResponse(value: persistentState.stateMachine[request.key])
    }

    /// Handles a GetState RPC.
    ///
    /// - Returns: The ServerStateResponse.
    public func getState() async -> ServerStateResponse {
        ServerStateResponse(
            id: persistentState.ownPeer.id,
            state: volatileState.state,
        )
    }

    /// Handles a GetTerm RPC.
    ///
    /// - Returns: The ServerTermResponse.
    public func getTerm() async -> ServerTermResponse {
        ServerTermResponse(
            id: persistentState.ownPeer.id,
            term: persistentState.currentTerm,
        )
    }

    /// Handles a GetImplementationVersion RPC.
    ///
    /// - Returns: The ImplementationVersionResponse.
    public func getImplementationVersion() async -> ImplementationVersionResponse {
        ImplementationVersionResponse(
            id: persistentState.ownPeer.id,
            implementation: "Swift",
            version: "1.2.0",
        )
    }

    // MARK: - Node Lifecycle

    /// Starts the node.
    public func start() async {
        await loadSnapshotOnStartup()
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

        let heartbeatInterval = Duration.milliseconds(persistentState.config.heartbeatInterval)

        let task = Task {
            while !Task.isCancelled {
                do {
                    let timeout: Duration
                    let action: @Sendable () async throws -> Void

                    if volatileState.state == .leader {
                        timeout = heartbeatInterval
                        action = self.sendHeartbeat
                    } else {
                        timeout = heartbeatInterval * 3
                        action = self.checkElectionTimeout
                    }

                    let actionTask = Task {
                        try await action()
                    }

                    try await Task.sleep(for: timeout)
                    actionTask.cancel()

                } catch {
                    logger.error("Heartbeat task failed: \(error)")
                    continue
                }
            }
            logger.critical("Heartbeat task cancelled")
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

        let currentTerm = persistentState.currentTerm
        let candidateID = persistentState.ownPeer.id
        let lastLogIndex = persistentState.log.count + persistentState.snapshot.lastIncludedIndex
        let lastLogTerm = persistentState.log.last?.term ?? persistentState.snapshot.lastIncludedTerm
        let peers = persistentState.peers

        await withTaskGroup { group in
            for peer in peers {
                group.addTask {
                    await self.requestVoteFromPeer(
                        peer: peer,
                        voteRequest: RequestVoteRequest(
                            term: currentTerm,
                            candidateID: candidateID,
                            lastLogIndex: lastLogIndex,
                            lastLogTerm: lastLogTerm,
                        ),
                    )
                }
            }

            for await (peerId, vote) in group {
                logger.trace("Received vote from \(peerId): \(vote.voteGranted)")

                // Check if the peer has a higher term
                if vote.term > persistentState.currentTerm {
                    logger.info("Received higher term \(vote.term), becoming follower of \(peerId)")
                    becomeFollower(newTerm: vote.term, currentLeaderId: peerId)
                    return
                }

                // Count votes only if we're still a candidate and in the same term
                if volatileState.state == .candidate, vote.term == persistentState.currentTerm, vote.voteGranted {
                    votes += 1

                    if votes >= requiredVotes {
                        logger.info("Received majority of votes (\(votes) / \(peers.count + 1)), becoming leader")
                        becomeLeader()

                        return
                    }
                }
            }

            if volatileState.state == .candidate {
                logger.info("Election failed, received votes: \(votes) / \(peers.count + 1)")
            }
        }
    }

    /// Requests a vote from a peer.
    ///
    /// - Parameters:
    ///   - peer: The peer to request a vote from.
    ///   - voteRequest: The vote request.
    /// - Returns: A tuple containing the peer ID and the vote response.
    func requestVoteFromPeer(
        peer: Peer,
        voteRequest: RequestVoteRequest,
    ) async -> (Int, RequestVoteResponse) {
        logger.trace("Requesting vote from \(peer.id)")
        do {
            let response = try await transport.requestVote(
                voteRequest,
                to: peer,
                isolation: #isolation,
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

        let currentTerm = persistentState.currentTerm
        let leaderID = persistentState.ownPeer.id
        let peers = persistentState.peers
        let commitIndex = volatileState.commitIndex
        let originalLogLength = persistentState.log.count

        // Add log entries to log
        persistentState.log.append(contentsOf: entries)

        // Create replication tracker with leader pre-marked as successful
        let replicationTracker = ReplicationTracker(majority: majority)
        logger.trace("Replicating log entries to peers", metadata: [
            "majority": .stringConvertible(majority),
            "entries": .stringConvertible(entries),
        ])
        await replicationTracker.markSuccess(id: leaderID)

        // Start a background task for replication
        Task {
            try await withThrowingTaskGroup { group in
                // Start individual replication tasks for each peer
                for peer in peers {
                    group.addTask {
                        try await self.replicateLogToPeer(
                            peer: peer,
                            replicationTracker: replicationTracker,
                            currentTerm: currentTerm,
                            leaderID: leaderID,
                            commitIndex: commitIndex,
                            originalLogLength: originalLogLength,
                            entries: entries,
                        )
                    }
                }

                try await group.waitForAll()
            }
        }

        // Wait for majority to have replicated the log before returning
        await replicationTracker.waitForMajority()
        logger.trace("Majority of peers have replicated the log", metadata: [
            "messageHash": .stringConvertible(entries.hashValue),
        ])

        // Once majority has replicated the log, update commit index
        // and apply committed entries
        if volatileState.state == .leader, persistentState.currentTerm == currentTerm {
            await updateCommitIndexAndApply()
        }
    }

    /// Replicates log entries to a single peer.
    ///
    /// - Parameters:
    ///   - peer: The peer to replicate the log to.
    ///   - replicationTracker: The replication tracker.
    ///   - currentTerm: The current term.
    ///   - leaderID: The leader ID.
    ///   - commitIndex: The commit index.
    ///   - originalLogLength: The original log length.
    ///   - entries: The log entries to replicate.
    private func replicateLogToPeer(
        peer: Peer,
        replicationTracker: ReplicationTracker,
        currentTerm: Int,
        leaderID: Int,
        commitIndex: Int,
        originalLogLength: Int,
        entries: [LogEntry],
    ) async throws {
        var retryCount = 0

        let targetEndIndex = originalLogLength + entries.count

        // Continue trying until successful or no longer leader
        while !Task.isCancelled, volatileState.state == .leader, persistentState.currentTerm == currentTerm, persistentState.peers.contains(peer) {
            // Check if already successful (another task marked it as successful)
            if await replicationTracker.isSuccessful(id: peer.id) {
                return
            }

            let currentMatchIndex = leaderState.matchIndex[peer.id] ?? 0
            if currentMatchIndex > targetEndIndex {
                await replicationTracker.markSuccess(id: peer.id)
                return
            }

            // Check if peer needs a snapshot
            let peerNextIndex = leaderState.nextIndex[peer.id] ?? persistentState.log.count + 1
            let firstLogIndex = persistentState.snapshot.lastIncludedIndex + 1

            if peerNextIndex < firstLogIndex {
                // Peer is too far behind, send snapshot
                logger.info("Peer \(peer.id) is too far behind (nextIndex: \(peerNextIndex), firstLogIndex: \(firstLogIndex)), sending snapshot")
                try await sendSnapshotToPeer(peer)
                // After snapshot, replication will continue in next iteration
                continue
            }

            do {
                let peerPrevLogIndex = peerNextIndex - 1
                let peerPrevLogTerm: Int

                if peerPrevLogIndex == 0 {
                    peerPrevLogTerm = 0
                } else if peerPrevLogIndex == persistentState.snapshot.lastIncludedIndex {
                    peerPrevLogTerm = persistentState.snapshot.lastIncludedTerm
                } else if peerPrevLogIndex > firstLogIndex - 1 {
                    let logArrayIndex = peerPrevLogIndex - firstLogIndex
                    peerPrevLogTerm = persistentState.log[logArrayIndex].term
                } else {
                    // This shouldn't happen after snapshot check above
                    logger.error("Invalid prevLogIndex \(peerPrevLogIndex) for peer \(peer.id)")
                    leaderState.nextIndex[peer.id] = firstLogIndex
                    continue
                }

                // Calculate entries to send
                let entriesToSend: [LogEntry]
                if peerNextIndex <= originalLogLength {
                    // Peer needs entries from the original log (catch-up scenario)
                    let startArrayIndex = peerNextIndex - firstLogIndex
                    if startArrayIndex >= 0, startArrayIndex < persistentState.log.count {
                        entriesToSend = Array(persistentState.log[startArrayIndex...])
                    } else {
                        entriesToSend = []
                    }
                } else if peerNextIndex == originalLogLength + 1 {
                    // Peer is up-to-date with original log, send only new entries
                    entriesToSend = entries
                } else {
                    // Peer's nextIndex is beyond what we expect - this shouldn't happen
                    // Reset nextIndex and retry
                    leaderState.nextIndex[peer.id] = originalLogLength + 1
                    continue
                }

                logger.trace("Sending append entries to \(peer.id) with nextIndex: \(peerNextIndex), prevLogIndex: \(peerPrevLogIndex), prevLogTerm: \(peerPrevLogTerm), entries.count: \(entriesToSend.count)")

                let appendEntriesRequest = AppendEntriesRequest(
                    term: currentTerm,
                    leaderID: leaderID,
                    prevLogIndex: peerPrevLogIndex,
                    prevLogTerm: peerPrevLogTerm,
                    entries: entriesToSend,
                    leaderCommit: commitIndex,
                )

                let result = try await transport.appendEntries(appendEntriesRequest, to: peer, isolation: #isolation)

                if Task.isCancelled {
                    return
                }

                if result.term > persistentState.currentTerm {
                    logger.info("Received higher term \(result.term), becoming follower of \(peer.id)")
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
        // Safety check: ensure we are still leader
        guard volatileState.state == .leader else {
            return
        }

        // Add own match index (implicitly the end of the log)
        var allMatchIndices = leaderState.matchIndex
        allMatchIndices[persistentState.ownPeer.id] = persistentState.log.count

        // Calculate new commit index based on majority match indices
        let sortedIndices = Array(allMatchIndices.values).sorted(by: >)
        let majorityIndex = sortedIndices[majority - 1]

        // Only update commit index if it's in the current term
        // (Raft safety requirement: only commit entries from current term)
        let oldCommitIndex = volatileState.commitIndex

        for newCommitIndex in stride(from: majorityIndex, through: oldCommitIndex + 1, by: -1) {
            if newCommitIndex > 0, newCommitIndex <= persistentState.log.count {
                let entry = persistentState.log[newCommitIndex - 1]
                if entry.term == persistentState.currentTerm {
                    volatileState.commitIndex = newCommitIndex
                    logger.trace("Updated commit index from \(oldCommitIndex) to \(newCommitIndex)")
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
                let oldValue = persistentState.stateMachine[key]
                persistentState.stateMachine[key] = entry.value
                logger.trace("Applied entry at index \(volatileState.lastApplied + 1): \(key) = \(entry.value ?? "nil") (was: \(oldValue ?? "nil"))")
            }

            volatileState.lastApplied += 1

            // Check if we should create a snapshot after applying entries
            if shouldCreateSnapshot() {
                Task {
                    do {
                        try await createSnapshot()
                    } catch {
                        logger.error("Failed to create snapshot: \(error)")
                    }
                }
            }
        }
    }

    /// Checks if the log is at least as up to date as the given log.
    ///
    /// - Parameters:
    ///   - lastLogIndex: The index of other node's last log entry.
    ///   - lastLogTerm: The term of other node's last log entry.
    private func isLogAtLeastAsUpToDate(lastLogIndex: Int, lastLogTerm: Int) -> Bool {
        let localLastLogTerm = persistentState.log.last?.term ?? persistentState.snapshot.lastIncludedTerm

        if lastLogTerm != localLastLogTerm {
            return lastLogTerm > localLastLogTerm
        }

        let localLastLogIndex = persistentState.log.count + persistentState.snapshot.lastIncludedIndex
        return lastLogIndex >= localLastLogIndex
    }

    // MARK: - Snapshots

    /// Loads snapshot from disk during startup (add this to your init or start method)
    private func loadSnapshotOnStartup() async {
        do {
            if let snapshot = try await persistence.loadSnapshot(for: persistentState.ownPeer.id) {
                persistentState.snapshot = snapshot
                persistentState.currentTerm = snapshot.lastIncludedTerm
                persistentState.stateMachine = snapshot.stateMachine
                volatileState.commitIndex = snapshot.lastIncludedIndex
                volatileState.lastApplied = snapshot.lastIncludedIndex

                logger.info("Loaded snapshot from disk up to index \(snapshot.lastIncludedIndex)")
            }
        } catch {
            logger.warning("Failed to load snapshot from disk: \(error)")
        }
    }

    /// Checks if a snapshot should be created based on log size
    private func shouldCreateSnapshot() -> Bool {
        let logSize = persistentState.log.count
        let threshold = persistence.compactionThreshold

        // return logSize >= threshold
        let result = logSize >= threshold
        if result {
            print("shouldCreateSnapshot: \(result), logSize: \(logSize), threshold: \(threshold)")
        }
        return result
    }

    /// Creates a snapshot of the current state
    private func createSnapshot() async throws {
        if isSnapshotting {
            return
        }
        isSnapshotting = true
        defer {
            isSnapshotting = false
        }

        let logSize = persistentState.log.count
        let lastIncludedIndex = volatileState.lastApplied
        guard lastIncludedIndex > 0 else {
            logger.trace("No entries to snapshot yet")
            return
        }

        let lastIncludedTerm = if lastIncludedIndex <= persistentState.log.count {
            persistentState.log[lastIncludedIndex - 1].term
        } else {
            // Entry is in existing snapshot
            persistentState.snapshot.lastIncludedTerm
        }

        let snapshot = Snapshot(
            lastIncludedIndex: lastIncludedIndex,
            lastIncludedTerm: lastIncludedTerm,
            stateMachine: persistentState.stateMachine,
        )

        // Save snapshot
        try await persistence.saveSnapshot(snapshot, for: persistentState.ownPeer.id)
        persistentState.snapshot = snapshot

        print("persistentState.log.count: \(persistentState.log.count), lastIncludedIndex: \(lastIncludedIndex)")
        persistentState.log.removeSubrange(0 ..< logSize)
        logger.info("Created snapshot up to index \(lastIncludedIndex), trimmed \(logSize) log entries")
    }

    /// Sends a snapshot to a peer that is too far behind
    private func sendSnapshotToPeer(_ peer: Peer) async throws {
        logger.info("Sending snapshot to \(peer.id) up to index \(persistentState.snapshot.lastIncludedIndex)")

        let request = InstallSnapshotRequest(
            term: persistentState.currentTerm,
            leaderID: persistentState.ownPeer.id,
            snapshot: persistentState.snapshot,
        )

        do {
            let response = try await transport.installSnapshot(request, on: peer, isolation: #isolation)

            if response.term > persistentState.currentTerm {
                logger.info("Received higher term \(response.term), becoming follower")
                becomeFollower(newTerm: response.term, currentLeaderId: peer.id)
                return
            }

            // Update peer's indices after successful snapshot installation
            leaderState.nextIndex[peer.id] = request.snapshot.lastIncludedIndex + 1
            leaderState.matchIndex[peer.id] = request.snapshot.lastIncludedIndex

            logger.info("Successfully sent snapshot to \(peer.id)")

        } catch {
            logger.error("Failed to send snapshot to \(peer.id): \(error)")
            throw error
        }
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
