import Foundation
import Logging

public final class RaftNodeManualLock: @unchecked Sendable, RaftNodeProtocol {
    // MARK: - Properties

    private let lock = NSLock()

    /// The transport layer for peer-to-peer communication.
    private let transport: any RaftNodeTransport
    /// The logger for logging messages.
    let logger: Logger
    /// The metrics collector for collecting metrics.
    let metricsCollector: MetricsCollector?

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
    public init(
        _ ownPeer: Peer,
        peers: [Peer],
        config: RaftConfig,
        transport: any RaftNodeTransport,
        persistence: any RaftNodePersistence,
        collectMetrics: Bool
    ) {
        self.transport = transport
        let newLogger = Logger(label: "raft.RaftNode.\(ownPeer.id)")
        logger = newLogger

        metricsCollector = collectMetrics ? try? MetricsCollector(interval: .milliseconds(250), maxSamples: 1000) : nil

        persistentState = PersistentState(
            ownPeer: ownPeer,
            peers: peers,
            config: config,
            persistence: persistence,
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
        lock.withLock {
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
    }

    /// Handles an AppendEntries RPC.
    ///
    /// - Parameters:
    ///   - request: The AppendEntriesRequest to handle.
    /// - Returns: The AppendEntriesResponse.
    public func appendEntries(request: AppendEntriesRequest) async -> AppendEntriesResponse {
        lock.withLock {
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
                if persistentState.logLength < request.prevLogIndex {
                    logger.info("Log is too short (length: \(persistentState.logLength), needed: \(request.prevLogIndex))")

                    return AppendEntriesResponse(
                        term: persistentState.currentTerm,
                        success: false,
                    )
                }

                let prevLogTerm = if request.prevLogIndex > persistentState.snapshot.lastIncludedIndex {
                    persistentState.log[request.prevLogIndex - persistentState.snapshot.lastIncludedIndex - 1].term
                } else {
                    persistentState.snapshot.lastIncludedTerm
                }
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
                    let logIndex = request.prevLogIndex + i + 1 // The absolute index of the log entry
                    let arrayIndex = logIndex - persistentState.snapshot.lastIncludedIndex - 1 // The index in the log array

                    if arrayIndex >= 0, arrayIndex < persistentState.log.count {
                        // This is an existing entry in our log - check for conflict
                        let existingEntry = persistentState.log[arrayIndex]
                        if existingEntry.term != newEntry.term {
                            // Found a conflict - different term for same index
                            logger.info("Found conflict at index \(logIndex): existing term \(existingEntry.term), new term \(newEntry.term)")
                            conflictIndex = i
                            break
                        }

                        // Entry matches, will be replicated correctly
                    } else if arrayIndex < 0 {
                        // The new entry's log index is covered by the snapshot, which means a conflict.
                        // This can happen if the leader's snapshot is older than ours,
                        // or if it's trying to send entries that are already in our snapshot.
                        logger.info("New entry at index \(logIndex) is already covered by snapshot.")
                        conflictIndex = i
                    } else {
                        // We've reached the end of our log - remaining entries are new
                        break
                    }
                }

                if let conflictIndex {
                    // Remove conflicting entry and everything that follows
                    let deleteFromIndex = request.prevLogIndex + conflictIndex + 1 // The absolute index of the log entry
                    let deleteFromArrayIndex = deleteFromIndex - persistentState.snapshot.lastIncludedIndex - 1 // The index in the log array

                    if deleteFromArrayIndex >= 0 {
                        logger.info("Truncating log from absolute index \(deleteFromIndex) (array index \(deleteFromArrayIndex))")
                        persistentState.log.removeSubrange(deleteFromArrayIndex ..< persistentState.log.count)
                    } else {
                        // The conflict is within the snapshot range or immediately after.
                        // In this case, we should discard the entire log because our log is inconsistent
                        // with what the leader is sending regarding its snapshot base.
                        logger.info("Truncating entire log due to conflict detected within or immediately after snapshot range.")
                        persistentState.log.removeAll(keepingCapacity: true)
                    }
                }

                // Append any new entries not already in the log
                let startAppendIndex = max(0, persistentState.logLength - request.prevLogIndex)
                if startAppendIndex < request.entries.count {
                    let entriesToAppend = request.entries[startAppendIndex...]
                    logger.debug("Appending \(entriesToAppend.count) entries starting from log index \(persistentState.logLength + 1)")
                    persistentState.log.append(contentsOf: entriesToAppend)
                }
            }

            // Update commit index
            if request.leaderCommit > volatileState.commitIndex {
                let lastLogIndex = persistentState.logLength
                volatileState.commitIndex = min(request.leaderCommit, lastLogIndex)
                logger.debug("Updating commit index to \(volatileState.commitIndex)")

                applyCommittedEntries()
            }

            return AppendEntriesResponse(
                term: persistentState.currentTerm,
                success: true,
            )
        }
    }

    /// Handles an InstallSnapshot RPC.
    ///
    /// - Parameter request: The InstallSnapshotRequest to handle.
    /// - Returns: The InstallSnapshotResponse.
    public func installSnapshot(request: InstallSnapshotRequest) async throws -> InstallSnapshotResponse {
        let (shouldReturnEarly, term, ownPeerId, oldSnapshotLastIndex, currentTerm) = lock.withLock { () -> (Bool, Int, Int, Int, Int) in
            logger.trace("Received snapshot to install from \(request.leaderID)")
            resetElectionTimer()

            // Reply immediately if term < currentTerm
            if request.term < persistentState.currentTerm {
                return (true, persistentState.currentTerm, 0, 0, 0)
            }

            if request.term > persistentState.currentTerm {
                // Update term and become follower, if necessary
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

            let ownPeerId = persistentState.ownPeer.id
            let oldSnapshotLastIndex = persistentState.snapshot.lastIncludedIndex
            let currentTerm = persistentState.currentTerm
            return (false, 0, ownPeerId, oldSnapshotLastIndex, currentTerm)
        }

        if shouldReturnEarly {
            return InstallSnapshotResponse(term: term)
        }

        // Save snapshot
        try await persistentState.persistence.saveSnapshot(request.snapshot, for: ownPeerId)

        return lock.withLock {
            persistentState.snapshot = request.snapshot

            let snapshotLastIndex = persistentState.snapshot.lastIncludedIndex
            let snapshotLastTerm = persistentState.snapshot.lastIncludedTerm

            // Adjust the log based on the new snapshot
            // If an existing entry has the same index and term as the snapshot's
            // last included entry, we can keep the log entries that follow it.
            // Otherwise, we need to truncate the entire log.
            if oldSnapshotLastIndex < snapshotLastIndex {
                let logIndex = oldSnapshotLastIndex + persistentState.log.count - snapshotLastIndex

                if logIndex >= 0, persistentState.log[logIndex].term == snapshotLastTerm {
                    logger.info("Kept \(logIndex) log entries after installing snapshot")
                    persistentState.log.removeSubrange(0 ..< logIndex)
                } else {
                    logger.info("Discarded entire log due to conflict")
                    persistentState.log.removeAll(keepingCapacity: true)
                }
            } else {
                // Discard entire log since the new snapshot is older
                logger.info("Discarded entire log since new snapshot is older")
                persistentState.log.removeAll(keepingCapacity: true)
            }

            // Reset state machine using the snapshot
            persistentState.stateMachine = request.snapshot.stateMachine

            // Update commit and last applied indices to reflect the snapshot's state
            volatileState.commitIndex = snapshotLastIndex
            volatileState.lastApplied = snapshotLastIndex

            logger.info("Successfully installed snapshot up to index \(snapshotLastIndex)")
            return InstallSnapshotResponse(term: currentTerm)
        }
    }

    // MARK: - Client RPCs

    /// Handles a Put RPC.
    ///
    /// - Parameters:
    ///   - request: The PutRequest to handle.
    /// - Returns: The PutResponse.
    public func put(request: PutRequest) async throws -> PutResponse {
        let (state, leaderHint, currentTerm) = lock.withLock {
            (volatileState.state, persistentState.peers.first { $0.id == volatileState.currentLeaderID }, persistentState.currentTerm)
        }

        guard state == .leader else {
            return PutResponse(success: false, leaderHint: leaderHint)
        }

        try await replicateLog(entries: [LogEntry(term: currentTerm, key: request.key, value: request.value)])

        return PutResponse(success: true)
    }

    /// Handles a Get RPC.
    ///
    /// - Parameters:
    ///   - request: The GetRequest to handle.
    /// - Returns: The GetResponse.
    public func get(request: GetRequest) async -> GetResponse {
        lock.withLock {
            guard volatileState.state == .leader else {
                return GetResponse(leaderHint: persistentState.peers.first { $0.id == volatileState.currentLeaderID })
            }

            return GetResponse(value: persistentState.stateMachine[request.key])
        }
    }

    /// Handles a GetDebug RPC.
    ///
    /// - Parameters:
    ///   - request: The GetRequest to handle.
    /// - Returns: The GetResponse.
    public func getDebug(request: GetRequest) async -> GetResponse {
        lock.withLock {
            GetResponse(value: persistentState.stateMachine[request.key])
        }
    }

    /// Handles a GetState RPC.
    ///
    /// - Returns: The ServerStateResponse.
    public func getState() async -> ServerStateResponse {
        lock.withLock {
            ServerStateResponse(
                id: persistentState.ownPeer.id,
                state: volatileState.state,
            )
        }
    }

    /// Handles a GetTerm RPC.
    ///
    /// - Returns: The ServerTermResponse.
    public func getTerm() async -> ServerTermResponse {
        lock.withLock {
            ServerTermResponse(
                id: persistentState.ownPeer.id,
                term: persistentState.currentTerm,
            )
        }
    }

    /// Handles a GetDiagnostics RPC.
    ///
    /// - Parameters:
    ///   - request: The DiagnosticsRequest to handle.
    /// - Returns: The DiagnosticsResponse.
    public func getDiagnostics(request: DiagnosticsRequest) async -> DiagnosticsResponse {
        let samples: [MetricsSample]? = if let metricsCollector {
            await metricsCollector.getSamples(start: request.start, end: request.end)
        } else {
            nil
        }

        return lock.withLock {
            DiagnosticsResponse(
                id: persistentState.ownPeer.id,
                implementation: "Swift (Manual Locks)",
                version: "1.4.0",
                compactionThreshold: persistentState.persistence.compactionThreshold,
                metrics: samples,
            )
        }
    }

    // MARK: - Node Lifecycle

    /// Starts the node.
    public func start() async {
        await loadSnapshotOnStartup()
        startHeartbeatTask()
    }

    /// Shuts down the node.
    public func shutdown() {
        lock.withLock {
            heartbeatTask?.cancel()
            heartbeatTask = nil
        }
    }

    /// Starts the heartbeat task.
    ///
    /// If the node is a leader, it will send heartbeats to all followers.
    /// If the node is a follower, it will check the election timeout.
    private func startHeartbeatTask() {
        lock.withLock {
            // Cancel existing task if it exists
            heartbeatTask?.cancel()

            let heartbeatInterval = Duration.milliseconds(persistentState.config.heartbeatInterval)

            let task = Task {
                while !Task.isCancelled {
                    do {
                        let (timeout, action) = self.lock.withLock {
                            if self.volatileState.state == .leader {
                                (heartbeatInterval, self.sendHeartbeat)
                            } else {
                                (heartbeatInterval * 3, self.checkElectionTimeout)
                            }
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
            }

            heartbeatTask = task
        }
    }

    /// Sends a heartbeat to all followers.
    private func sendHeartbeat() async throws {
        let state = lock.withLock { volatileState.state }

        guard state == .leader else {
            throw RaftError.notLeader
        }

        logger.trace("Sending heartbeat to followers")
        try await replicateLog(entries: [])
    }

    // MARK: - Election

    /// Resets the election timer.
    private func resetElectionTimer() {
        // Assumes lock is held
        volatileState.lastHeartbeat = Date()
    }

    /// Checks if the election timeout has been reached.
    private func checkElectionTimeout() async throws {
        let (electionTimeout, lastHeartbeat) = lock.withLock {
            (volatileState.electionTimeout, volatileState.lastHeartbeat)
        }

        let now = Date()
        if now.timeIntervalSince(lastHeartbeat) * 1000 > Double(electionTimeout) {
            logger.info("Election timeout reached, becoming candidate")
            try await startElection()
        }
    }

    /// Starts an election.
    private func startElection() async throws {
        logger.trace("Starting election")
        lock.withLock {
            becomeCandidate()

            // Reset election timeout
            volatileState.electionTimeout = Int.random(in: persistentState.config.electionTimeoutRange)
            resetElectionTimer()
        }

        // Request votes from other nodes
        try await requestVotes()
    }

    /// Requests votes from all peers.
    private func requestVotes() async throws {
        let (requiredVotes, currentTerm, candidateID, lastLogIndex, lastLogTerm, peers) = lock.withLock {
            logger.trace("Requesting votes from peers: \(persistentState.peers)")
            let requiredVotes = majority
            let currentTerm = persistentState.currentTerm
            let candidateID = persistentState.ownPeer.id
            let lastLogIndex = persistentState.logLength
            let lastLogTerm = persistentState.log.last?.term ?? persistentState.snapshot.lastIncludedTerm
            let peers = persistentState.peers
            return (requiredVotes, currentTerm, candidateID, lastLogIndex, lastLogTerm, peers)
        }

        // Count own vote
        var votes = 1

        await withTaskGroup(of: (Int, RequestVoteResponse).self) { group in
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
                let shouldReturn = lock.withLock {
                    logger.trace("Received vote from \(peerId): \(vote.voteGranted)")

                    // Check if the peer has a higher term
                    if vote.term > persistentState.currentTerm {
                        logger.info("Received higher term \(vote.term), becoming follower of \(peerId)")
                        becomeFollower(newTerm: vote.term, currentLeaderId: peerId)
                        return true
                    }

                    // Count votes only if we're still a candidate and in the same term
                    if volatileState.state == .candidate, vote.term == persistentState.currentTerm, vote.voteGranted {
                        votes += 1

                        if votes >= requiredVotes {
                            logger.info("Received majority of votes (\(votes) / \(peers.count + 1)), becoming leader")
                            becomeLeader()
                            return true
                        }
                    }
                    return false
                }
                if shouldReturn { return }
            }

            lock.withLock {
                if volatileState.state == .candidate {
                    logger.info("Election failed, received votes: \(votes) / \(peers.count + 1)")
                }
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
        let (isLeader, currentTerm, leaderID, peers, commitIndex, originalLogLength, requiredVotes) = lock.withLock { () -> (Bool, Int, Int, [Peer], Int, Int, Int) in
            guard volatileState.state == .leader else {
                logger.info("Not a leader, not replicating log")
                return (false, 0, 0, [], 0, 0, 0)
            }

            resetElectionTimer()

            let currentTerm = persistentState.currentTerm
            let leaderID = persistentState.ownPeer.id
            let peers = persistentState.peers
            let commitIndex = volatileState.commitIndex
            let originalLogLength = persistentState.logLength

            // Add log entries to log
            persistentState.log.append(contentsOf: entries)

            let requiredVotes = majority
            return (true, currentTerm, leaderID, peers, commitIndex, originalLogLength, requiredVotes)
        }

        if !isLeader {
            throw RaftError.notLeader
        }

        // Create replication tracker with leader pre-marked as successful
        let replicationTracker = ReplicationTracker(majority: requiredVotes)
        logger.trace("Replicating log entries to peers", metadata: [
            "majority": .stringConvertible(requiredVotes),
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
        let shouldUpdate = lock.withLock {
            volatileState.state == .leader && persistentState.currentTerm == currentTerm
        }
        if shouldUpdate {
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
        while !Task.isCancelled {
            let shouldContinue = lock.withLock {
                volatileState.state == .leader && persistentState.currentTerm == currentTerm && persistentState.peers.contains(peer)
            }
            if !shouldContinue { return }

            // Check if already successful (another task marked it as successful)
            if await replicationTracker.isSuccessful(id: peer.id) {
                return
            }

            let (shouldReturn, peerNextIndex, snapshotLastIncludedIndex) = lock.withLock {
                if let currentMatchIndex = leaderState.matchIndex[peer.id], currentMatchIndex > targetEndIndex {
                    return (true, 0, 0)
                }
                let peerNextIndex = leaderState.nextIndex[peer.id] ?? originalLogLength + 1
                let snapshotLastIncludedIndex = persistentState.snapshot.lastIncludedIndex
                return (false, peerNextIndex, snapshotLastIncludedIndex)
            }

            if shouldReturn {
                await replicationTracker.markSuccess(id: peer.id)
                return
            }

            // Check if peer needs a snapshot
            if peerNextIndex <= snapshotLastIncludedIndex {
                // Peer is too far behind, send snapshot
                logger.info("Peer \(peer.id) is too far behind (nextIndex: \(peerNextIndex)), sending snapshot. Snapshot last included index: \(snapshotLastIncludedIndex)")
                try await sendSnapshotToPeer(peer)

                lock.withLock {
                    // After sending snapshot, update nextIndex and matchIndex
                    leaderState.nextIndex[peer.id] = snapshotLastIncludedIndex + 1
                    leaderState.matchIndex[peer.id] = snapshotLastIncludedIndex
                }

                // Continue to next iteration to try sending append entries from the new nextIndex
                continue
            }

            let (shouldContinueLoop, entriesToSend, peerPrevLogIndex, peerPrevLogTerm) = lock.withLock { () -> (Bool, [LogEntry], Int, Int) in
                let peerNextIndex = leaderState.nextIndex[peer.id] ?? originalLogLength + 1
                // Calculate entries to send
                let entriesToSend: [LogEntry]
                if peerNextIndex <= originalLogLength {
                    // Peer needs entries from the original log (catch-up scenario)
                    let startIndex = peerNextIndex - persistentState.snapshot.lastIncludedIndex - 1
                    guard startIndex >= 0, startIndex < persistentState.log.count else {
                        logger.error("Invalid startIndex \(startIndex) for peer \(peer.id), log.count: \(persistentState.log.count)")
                        leaderState.nextIndex[peer.id] = persistentState.snapshot.lastIncludedIndex + 1
                        return (true, [], 0, 0)
                    }
                    entriesToSend = Array(persistentState.log[startIndex...])
                } else if peerNextIndex == originalLogLength + 1 {
                    // Peer is up-to-date with original log, send only new entries
                    entriesToSend = entries
                } else {
                    // Peer's nextIndex is beyond what we expect - this shouldn't happen
                    // Reset nextIndex and retry
                    logger.warning("Peer \(peer.id) has nextIndex \(peerNextIndex) which is beyond what we expect (originalLogLength: \(originalLogLength))")
                    leaderState.nextIndex[peer.id] = originalLogLength + 1
                    return (true, [], 0, 0)
                }

                let peerPrevLogIndex = peerNextIndex - 1
                let peerPrevLogTerm = if peerPrevLogIndex > persistentState.snapshot.lastIncludedIndex {
                    // prevLogIndex is after the snapshot, so we need to look it up in the log
                    persistentState.log[peerPrevLogIndex - persistentState.snapshot.lastIncludedIndex - 1].term
                } else {
                    // prevLogIndex is before the snapshot, so we need to look it up in the snapshot
                    persistentState.snapshot.lastIncludedTerm
                }
                return (false, entriesToSend, peerPrevLogIndex, peerPrevLogTerm)
            }

            if shouldContinueLoop {
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

            do {
                let result = try await transport.appendEntries(appendEntriesRequest, to: peer)

                if Task.isCancelled {
                    return
                }

                let (shouldReturnAfterFollower, success) = lock.withLock {
                    if result.term > persistentState.currentTerm {
                        logger.info("Received higher term \(result.term), becoming follower of \(peer.id)")
                        becomeFollower(newTerm: result.term, currentLeaderId: peer.id)
                        return (true, false)
                    }

                    if result.success {
                        logger.trace("Append entries successful for \(peer.id)", metadata: [
                            "messageHash": .stringConvertible(entriesToSend.hashValue),
                            "nextIndex": .stringConvertible(leaderState.nextIndex[peer.id] ?? 0),
                            "matchIndex": .stringConvertible(leaderState.matchIndex[peer.id] ?? 0),
                        ])

                        let newMatchIndex = peerPrevLogIndex + entriesToSend.count
                        leaderState.matchIndex[peer.id] = newMatchIndex
                        leaderState.nextIndex[peer.id] = newMatchIndex + 1
                        return (false, true)
                    } else {
                        // Log inconsistency, decrement nextIndex and retry
                        leaderState.nextIndex[peer.id] = max(1, (leaderState.nextIndex[peer.id] ?? 1) - 1)
                        retryCount += 1
                        logger.info("Append entries failed for \(peer.id), retrying with earlier index, retrying with index \(leaderState.nextIndex[peer.id] ?? 0)", metadata: [
                            "messageHash": .stringConvertible(entries.hashValue),
                            "retryCount": .stringConvertible(retryCount),
                            "nextIndex": .stringConvertible(leaderState.nextIndex[peer.id] ?? 0),
                        ])
                        return (false, false)
                    }
                }

                if shouldReturnAfterFollower {
                    return
                }

                if success {
                    await replicationTracker.markSuccess(id: peer.id)
                    return
                } else {
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
        lock.withLock {
            // Safety check: ensure we are still leader
            guard volatileState.state == .leader else {
                return
            }

            // Add own match index (implicitly the end of the log)
            var allMatchIndices = leaderState.matchIndex
            allMatchIndices[persistentState.ownPeer.id] = persistentState.logLength

            // Calculate new commit index based on majority match indices
            let sortedIndices = Array(allMatchIndices.values).sorted(by: >)
            let requiredVotes = majority
            let majorityIndex = sortedIndices[requiredVotes - 1]

            // Only update commit index if it's in the current term
            // (Raft safety requirement: only commit entries from current term)
            let oldCommitIndex = volatileState.commitIndex

            // Start from the majority index and work backwards to find the highest
            // committable index in the current term
            for newCommitIndex in stride(from: majorityIndex, through: oldCommitIndex + 1, by: -1) {
                // Check if this index is within the snapshot range
                if newCommitIndex <= persistentState.snapshot.lastIncludedIndex {
                    // This index is covered by the snapshot, so it's already committed
                    // Don't update commitIndex as it should already be at least this value
                    break
                }

                // Calculate the array index in the current log
                let logArrayIndex = newCommitIndex - persistentState.snapshot.lastIncludedIndex - 1

                // Ensure the index is within bounds of the current log
                if logArrayIndex >= 0, logArrayIndex < persistentState.log.count {
                    let entry = persistentState.log[logArrayIndex]
                    // Only commit entries from the current term for safety
                    if entry.term == persistentState.currentTerm {
                        volatileState.commitIndex = newCommitIndex
                        logger.trace("Updated commit index from \(oldCommitIndex) to \(newCommitIndex)")
                        break
                    }
                    // If the entry is from an older term, continue checking lower indices
                } else {
                    // Index is out of bounds - this shouldn't happen with correct logic
                    logger.warning("Calculated newCommitIndex \(newCommitIndex) is out of bounds for log (length: \(persistentState.log.count), snapshot last index: \(persistentState.snapshot.lastIncludedIndex))")
                }
            }

            // Apply newly committed entries to state machine
            applyCommittedEntries()
        }
    }

    /// Applies the committed entries to the state machine.
    private func applyCommittedEntries() {
        // Assumes lock is held
        while volatileState.lastApplied < volatileState.commitIndex {
            let nextApplyIndex = volatileState.lastApplied + 1

            if nextApplyIndex <= persistentState.snapshot.lastIncludedIndex {
                volatileState.lastApplied = nextApplyIndex
                continue
            }

            let logArrayIndex = nextApplyIndex - persistentState.snapshot.lastIncludedIndex - 1

            guard logArrayIndex >= 0, logArrayIndex < persistentState.log.count else {
                logger.warning("Cannot apply entry at index \(nextApplyIndex): log array index \(logArrayIndex) is out of bounds (log count: \(persistentState.log.count), snapshot last index: \(persistentState.snapshot.lastIncludedIndex))")
                break
            }

            let entry = persistentState.log[logArrayIndex]

            if let key = entry.key {
                let oldValue = persistentState.stateMachine[key]
                persistentState.stateMachine[key] = entry.value
                logger.trace("Applied entry at index \(volatileState.lastApplied + 1): \(key) = \(entry.value ?? "nil") (was: \(oldValue ?? "nil"))")
            }

            volatileState.lastApplied += 1
        }

        if shouldCreateSnapshot() {
            Task {
                try await createSnapshot()
            }
        }
    }

    /// Checks if the log is at least as up to date as the given log.
    ///
    /// - Parameters:
    ///   - lastLogIndex: The index of other node's last log entry.
    ///   - lastLogTerm: The term of other node's last log entry.
    private func isLogAtLeastAsUpToDate(lastLogIndex: Int, lastLogTerm: Int) -> Bool {
        // Assumes lock is held
        let localLastLogTerm = persistentState.log.last?.term ?? persistentState.snapshot.lastIncludedTerm

        if lastLogTerm != localLastLogTerm {
            return lastLogTerm > localLastLogTerm
        }

        let localLastLogIndex = persistentState.logLength
        return lastLogIndex >= localLastLogIndex
    }

    // MARK: - Snapshotting

    /// Loads the snapshot on startup of the node.
    private func loadSnapshotOnStartup() async {
        do {
            let ownPeerId = persistentState.ownPeer.id
            if let snapshot = try await persistentState.persistence.loadSnapshot(for: ownPeerId) {
                lock.withLock {
                    persistentState.snapshot = snapshot
                    persistentState.currentTerm = snapshot.lastIncludedTerm
                    persistentState.stateMachine = snapshot.stateMachine
                    volatileState.commitIndex = snapshot.lastIncludedIndex
                    volatileState.lastApplied = snapshot.lastIncludedIndex
                    logger.info("Successfully loaded snapshot up to index \(snapshot.lastIncludedIndex).")
                }
            }
        } catch {
            logger.error("Failed to load snapshot: \(error)")
        }
    }

    /// Checks if a snapshot should be created.
    ///
    /// - Returns: True if a snapshot should be created, false otherwise.
    private func shouldCreateSnapshot() -> Bool {
        // Assumes lock is held
        guard persistentState.persistence.compactionThreshold > 0 else {
            return false
        }

        return (volatileState.commitIndex - persistentState.snapshot.lastIncludedIndex) >= persistentState.persistence.compactionThreshold && !persistentState.isSnapshotting
    }

    /// Creates a snapshot.
    private func createSnapshot() async throws {
        let (shouldReturn, snapshot, ownPeerId, lastCommittedArrayIndex) = lock.withLock { () -> (Bool, Snapshot?, Int, Int) in
            guard !persistentState.isSnapshotting else {
                return (true, nil, 0, 0)
            }

            persistentState.isSnapshotting = true

            let snapshotLastIndex = volatileState.commitIndex
            guard snapshotLastIndex > persistentState.snapshot.lastIncludedIndex else {
                persistentState.isSnapshotting = false
                return (true, nil, 0, 0)
            }

            let lastCommittedArrayIndex = snapshotLastIndex - persistentState.snapshot.lastIncludedIndex - 1
            guard lastCommittedArrayIndex >= 0, lastCommittedArrayIndex < persistentState.log.count else {
                persistentState.isSnapshotting = false
                return (true, nil, 0, 0)
            }

            let snapshotLastTerm = persistentState.log[lastCommittedArrayIndex].term

            let snapshot = Snapshot(
                lastIncludedIndex: snapshotLastIndex,
                lastIncludedTerm: snapshotLastTerm,
                stateMachine: persistentState.stateMachine,
            )
            let ownPeerId = persistentState.ownPeer.id
            return (false, snapshot, ownPeerId, lastCommittedArrayIndex)
        }

        if shouldReturn { return }
        guard let snapshot else { return }

        try await persistentState.persistence.saveSnapshot(snapshot, for: ownPeerId)

        lock.withLock {
            persistentState.snapshot = snapshot

            // Truncate the log after saving snapshot
            let entriesToKeep = lastCommittedArrayIndex + 1
            persistentState.log.removeSubrange(0 ..< entriesToKeep)
            persistentState.isSnapshotting = false
        }

        logger.info("Created snapshot up to index \(snapshot.lastIncludedIndex).")
    }

    /// Sends a snapshot to a specific peer.
    ///
    /// - Parameter peer: The peer to send the snapshot to.
    private func sendSnapshotToPeer(_ peer: Peer) async throws {
        let (shouldReturn, currentTerm, leaderID, snapshot) = lock.withLock { () -> (Bool, Int, Int, Snapshot?) in
            guard volatileState.state == .leader, persistentState.isSendingSnapshot[peer.id] != true else {
                return (true, 0, 0, nil)
            }

            persistentState.isSendingSnapshot[peer.id] = true
            let currentTerm = persistentState.currentTerm
            let leaderID = persistentState.ownPeer.id
            let snapshot = persistentState.snapshot
            return (false, currentTerm, leaderID, snapshot)
        }

        if shouldReturn { return }
        guard let snapshot else { return }

        logger.info("Sending InstallSnapshot RPC to \(peer.id) (snapshot last index: \(snapshot.lastIncludedIndex), last term: \(snapshot.lastIncludedTerm))")

        let request = InstallSnapshotRequest(
            term: currentTerm,
            leaderID: leaderID,
            snapshot: snapshot,
        )

        do {
            let response = try await transport.installSnapshot(request, on: peer)

            let shouldReturnAfterFollower = lock.withLock {
                persistentState.isSendingSnapshot[peer.id] = false

                if response.term > persistentState.currentTerm {
                    logger.info("Received higher term \(response.term) during InstallSnapshot, becoming follower of \(peer.id)")
                    becomeFollower(newTerm: response.term, currentLeaderId: peer.id)
                    return true
                }

                // Upon successful installation, update matchIndex and nextIndex for the peer
                leaderState.matchIndex[peer.id] = snapshot.lastIncludedIndex
                leaderState.nextIndex[peer.id] = snapshot.lastIncludedIndex + 1
                logger.info("Successfully sent snapshot to \(peer.id). Updated nextIndex to \(leaderState.nextIndex[peer.id] ?? 0) and matchIndex to \(leaderState.matchIndex[peer.id] ?? 0)")
                return false
            }
            if shouldReturnAfterFollower { return }
        } catch {
            logger.error("Failed to send snapshot to \(peer.id): \(error)")
        }
    }

    // MARK: - State Changes

    /// Let the node become a follower.
    ///
    /// - Parameters:
    ///   - newTerm: The new term.
    ///   - currentLeaderId: The ID of the current leader.
    private func becomeFollower(newTerm: Int, currentLeaderId: Int) {
        // Assumes lock is held
        let oldState = volatileState.state
        if oldState != .follower {
            logger.info("Transitioning from \(oldState) to follower for term \(newTerm)")
        }

        volatileState.state = .follower
        persistentState.currentTerm = newTerm
        persistentState.votedFor = nil
        volatileState.currentLeaderID = currentLeaderId
        resetElectionTimer()
    }

    /// Let the node become a candidate.
    private func becomeCandidate() {
        // Assumes lock is held
        logger.info("Transitioning to candidate for term \(persistentState.currentTerm + 1)")
        volatileState.state = .candidate
        persistentState.currentTerm += 1
        persistentState.votedFor = persistentState.ownPeer.id
        volatileState.currentLeaderID = nil
    }

    /// Let the node become a leader.
    private func becomeLeader() {
        // Assumes lock is held
        logger.info("Transitioning to leader for term \(persistentState.currentTerm)")
        volatileState.state = .leader
        volatileState.currentLeaderID = persistentState.ownPeer.id

        // Clear and reinitialize maps
        leaderState.nextIndex = [:]
        leaderState.matchIndex = [:]

        // Initialize nextIndex and matchIndex for all peers
        for peer in persistentState.peers {
            leaderState.nextIndex[peer.id] = persistentState.logLength + 1 // Next log index to send to that server
            leaderState.matchIndex[peer.id] = 0 // Highest log entry known to be replicated on server
        }
    }
}

extension RaftNodeManualLock: Identifiable {
    public var id: Int {
        persistentState.ownPeer.id
    }
}
