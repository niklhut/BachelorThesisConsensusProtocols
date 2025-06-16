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
            if request.prevLogIndex > persistentState.snapshot.lastIncludedIndex {
                // If prevLogIndex is beyond the snapshot, it must be in the current log
                let logIndex = request.prevLogIndex - persistentState.snapshot.lastIncludedIndex - 1
                if persistentState.log.count <= logIndex || persistentState.log[logIndex].term != request.prevLogTerm {
                    logger.info("Log inconsistency: prevLogIndex \(request.prevLogIndex) not found or term mismatch (log length: \(persistentState.log.count), target index: \(logIndex))")
                    return AppendEntriesResponse(
                        term: persistentState.currentTerm,
                        success: false,
                    )
                }
            } else if request.prevLogIndex == persistentState.snapshot.lastIncludedIndex {
                // If prevLogIndex is the last index of the snapshot
                if persistentState.snapshot.lastIncludedTerm != request.prevLogTerm {
                    logger.info("Snapshot inconsistency: prevLogIndex matches snapshot last included index, but terms differ.")
                    return AppendEntriesResponse(
                        term: persistentState.currentTerm,
                        success: false,
                    )
                }
            } else {
                // prevLogIndex is before the snapshot, which is an inconsistency.
                // This implies the leader is trying to send entries that predate our snapshot.
                logger.info("Log inconsistency: prevLogIndex \(request.prevLogIndex) is before snapshot last included index \(persistentState.snapshot.lastIncludedIndex)")
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
                let logIndex = request.prevLogIndex + i + 1 // This is the absolute log index
                let arrayIndex = logIndex - persistentState.snapshot.lastIncludedIndex - 1 // This is the index in our current log array

                if arrayIndex >= 0, arrayIndex < persistentState.log.count {
                    // This is an existing entry in our log - check for conflict
                    let existingEntry = persistentState.log[arrayIndex]
                    if existingEntry.term != newEntry.term {
                        // Found a conflict - different term for same index
                        logger.info("Found conflict at index \(logIndex): existing term \(existingEntry.term), new term \(newEntry.term)")
                        conflictIndex = i
                        break
                    }
                } else if arrayIndex < 0 {
                    // The new entry's log index is covered by the snapshot, which means a conflict.
                    // This can happen if the leader's snapshot is older than ours,
                    // or if it's trying to send entries that are already in our snapshot.
                    logger.info("New entry at index \(logIndex) is already covered by snapshot.")
                    conflictIndex = i // Treat as a conflict to truncate at this point if needed
                    break
                } else {
                    // We've reached the end of our log - remaining entries are new
                    break
                }
            }

            if let conflictIndex {
                // Remove conflicting entry and everything that follows
                let deleteFromAbsoluteIndex = request.prevLogIndex + conflictIndex + 1
                let deleteFromArrayIndex = deleteFromAbsoluteIndex - persistentState.snapshot.lastIncludedIndex - 1

                if deleteFromArrayIndex >= 0 {
                    logger.info("Truncating log from absolute index \(deleteFromAbsoluteIndex) (array index: \(deleteFromArrayIndex))")
                    persistentState.log.removeSubrange(deleteFromArrayIndex ..< persistentState.log.count)
                } else {
                    // This means the conflict is within the snapshot range or immediately after.
                    // In this case, we should discard the entire log because our log is inconsistent
                    // with what the leader is sending regarding its snapshot base.
                    logger.info("Truncating entire log due to conflict detected within or immediately after snapshot range.")
                    persistentState.log = []
                }
            }

            // Append any new entries not already in the log
            let startAppendAbsoluteIndex = request.prevLogIndex + 1
            let startAppendArrayIndex = startAppendAbsoluteIndex - persistentState.snapshot.lastIncludedIndex - 1

            if startAppendArrayIndex <= request.entries.count {
                let entriesToAppend: [LogEntry] = if startAppendArrayIndex < 0 {
                    // This implies some entries in request.entries are covered by our snapshot.
                    // We only append entries that are truly new to our current log.
                    Array(request.entries[max(0, -startAppendArrayIndex)...])
                } else {
                    Array(request.entries[startAppendArrayIndex...])
                }

                logger.info("Appending \(entriesToAppend.count) entries starting from log absolute index \(persistentState.log.count + persistentState.snapshot.lastIncludedIndex + 1)")
                persistentState.log.append(contentsOf: entriesToAppend)
            } else {
                logger.error("Tried to append entries but request.entries.count is \(request.entries.count) and startAppendArrayIndex is \(startAppendArrayIndex)")
                return AppendEntriesResponse(
                    term: persistentState.currentTerm,
                    success: false,
                )
            }
        }

        // Update commit index
        if request.leaderCommit > volatileState.commitIndex {
            let lastLogIndex = persistentState.log.count + persistentState.snapshot.lastIncludedIndex
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

        // Adjust the log based on the new snapshot
        // If an existing log entry has the same index and term as the snapshot's
        // last included entry, we can keep the log entries that follow it.
        // Otherwise, discard the entire log.
        if persistentState.log.count + persistentState.snapshot.lastIncludedIndex >= snapshotLastIndex {
            let relativeIndex = snapshotLastIndex - persistentState.snapshot.lastIncludedIndex - 1
            if relativeIndex >= 0, persistentState.log[relativeIndex].term == snapshotLastTerm {
                // Keep log entries after the snapshot's last included entry
                persistentState.log.removeSubrange(0 ..< relativeIndex + 1)
                logger.info("Kept \(persistentState.log.count) log entries after installing snapshot (relative index: \(relativeIndex))")
            } else {
                // Discard entire log - conflict detected or snapshot covers existing log
                persistentState.log = []
                logger.info("Discarded entire log due to conflict with snapshot or snapshot covers existing log")
            }
        } else {
            // Log is shorter than snapshot - discard entire log
            persistentState.log = []
            logger.info("Discarded entire log - shorter than snapshot")
        }

        // Reset state machine using snapshot
        persistentState.stateMachine = request.snapshot.stateMachine

        // Update commit and last applied indices to reflect the snapshot's state
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
        // Ensure commitIndex and lastApplied are correctly set after loading snapshot
        volatileState.commitIndex = max(volatileState.commitIndex, persistentState.snapshot.lastIncludedIndex)
        volatileState.lastApplied = max(volatileState.lastApplied, persistentState.snapshot.lastIncludedIndex)
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
        // The last log index for vote request should be the absolute last index,
        // which includes entries in the snapshot.
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

        let targetEndIndex = persistentState.log.count + persistentState.snapshot.lastIncludedIndex // Absolute index of the last entry in the leader's log

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

            // Determine peer's nextIndex, ensuring it's at least the last included index + 1
            let peerNextIndex = leaderState.nextIndex[peer.id] ?? persistentState.log.count + persistentState.snapshot.lastIncludedIndex + 1
            // let peerNextIndex = max(persistentState.snapshot.lastIncludedIndex + 1, leaderState.nextIndex[peer.id] ?? (persistentState.log.count + persistentState.snapshot.lastIncludedIndex + 1))

            // Check if peer needs a snapshot
            if peerNextIndex <= persistentState.snapshot.lastIncludedIndex {
                // Peer is too far behind or its nextIndex suggests it's before our snapshot.
                // We should send the snapshot.
                logger.info("Peer \(peer.id) is too far behind (nextIndex: \(peerNextIndex)), sending snapshot. Snapshot last included index: \(persistentState.snapshot.lastIncludedIndex)")
                try await sendSnapshotToPeer(peer)

                // After sending snapshot, update nextIndex and matchIndex for this peer
                // to reflect the snapshot's last included index.
                leaderState.matchIndex[peer.id] = persistentState.snapshot.lastIncludedIndex
                leaderState.nextIndex[peer.id] = persistentState.snapshot.lastIncludedIndex + 1

                continue // Continue to next iteration to try sending append entries from the new nextIndex
            }

            do {
                let peerPrevLogIndex = peerNextIndex - 1
                let peerPrevLogTerm: Int

                if peerPrevLogIndex == 0 {
                    peerPrevLogTerm = 0
                } else if peerPrevLogIndex == persistentState.snapshot.lastIncludedIndex {
                    // prevLogIndex is exactly the last entry of the snapshot
                    peerPrevLogTerm = persistentState.snapshot.lastIncludedTerm
                } else if peerPrevLogIndex > persistentState.snapshot.lastIncludedIndex {
                    // prevLogIndex is after the snapshot, so it's in the current log
                    let logArrayIndex = peerPrevLogIndex - persistentState.snapshot.lastIncludedIndex - 1
                    if logArrayIndex >= 0, logArrayIndex < persistentState.log.count {
                        peerPrevLogTerm = persistentState.log[logArrayIndex].term
                    } else {
                        // This indicates an inconsistency. The leader thinks the follower needs
                        // an entry at prevLogIndex, but the leader doesn't have it in its current log
                        // (after accounting for the snapshot). This could happen if the leader's log
                        // was truncated due to a snapshot, but its nextIndex for this peer was not
                        // correctly adjusted.
                        logger.error("Leader's log is too short for peer's nextIndex calculation. Resetting nextIndex for \(peer.id).")
                        leaderState.nextIndex[peer.id] = persistentState.snapshot.lastIncludedIndex + 1
                        continue
                    }
                } else {
                    // This case should ideally be handled by the snapshot check above,
                    // but as a fallback, if peerPrevLogIndex is somehow less than lastIncludedIndex,
                    // it means the leader is trying to send entries that are covered by the snapshot.
                    // This indicates an inconsistency or a logic error in `nextIndex` management.
                    logger.error("Invalid prevLogIndex \(peerPrevLogIndex) for peer \(peer.id) relative to snapshot. Resetting nextIndex.")
                    leaderState.nextIndex[peer.id] = persistentState.snapshot.lastIncludedIndex + 1
                    continue
                }

                // Calculate entries to send
                let entriesToSend: [LogEntry]
                let startAbsoluteIndexForEntries = peerNextIndex
                let startRelativeIndexInLog = startAbsoluteIndexForEntries - persistentState.snapshot.lastIncludedIndex - 1

                if startRelativeIndexInLog < persistentState.log.count {
                    entriesToSend = Array(persistentState.log[max(0, startRelativeIndexInLog)...])
                } else {
                    entriesToSend = [] // No new entries to send beyond what's already in the log
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
                    // leaderState.nextIndex[peer.id] = max(persistentState.snapshot.lastIncludedIndex + 1, (leaderState.nextIndex[peer.id] ?? 1) - 1)
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
        // The leader's own "match index" is its current absolute log length
        allMatchIndices[persistentState.ownPeer.id] = persistentState.log.count + persistentState.snapshot.lastIncludedIndex

        // Calculate new commit index based on majority match indices
        let sortedIndices = Array(allMatchIndices.values).sorted(by: >)
        // Ensure there are enough indices to calculate a majority
        guard majority - 1 < sortedIndices.count else {
            logger.warning("Not enough match indices to determine majority for commit. Current indices: \(sortedIndices.count), majority needed: \(majority)")
            return
        }
        let majorityIndex = sortedIndices[majority - 1]

        // Only update commit index if it's in the current term
        // (Raft safety requirement: only commit entries from current term)
        let oldCommitIndex = volatileState.commitIndex

        // Iterate backwards from the new potential commit index down to old commit index + 1
        for newCommitIndex in stride(from: majorityIndex, through: oldCommitIndex + 1, by: -1) {
            // Ensure the newCommitIndex is greater than the snapshot's last included index
            // and within the bounds of the current log
            if newCommitIndex > persistentState.snapshot.lastIncludedIndex {
                let logArrayIndex = newCommitIndex - persistentState.snapshot.lastIncludedIndex - 1
                if logArrayIndex >= 0, logArrayIndex < persistentState.log.count {
                    let entry = persistentState.log[logArrayIndex]
                    if entry.term == persistentState.currentTerm {
                        volatileState.commitIndex = newCommitIndex
                        logger.trace("Updated commit index from \(oldCommitIndex) to \(newCommitIndex)")
                        break
                    }
                } else {
                    // This case indicates that newCommitIndex is out of bounds for the current log,
                    // implying a logic error in majorityIndex calculation or log management.
                    logger.warning("Calculated newCommitIndex \(newCommitIndex) is out of bounds for log (length: \(persistentState.log.count), snapshot last index: \(persistentState.snapshot.lastIncludedIndex))")
                }
            } else if newCommitIndex <= persistentState.snapshot.lastIncludedIndex {
                // If the majority index falls within or below the snapshot, it means all entries
                // up to the snapshot are considered committed.
                volatileState.commitIndex = max(volatileState.commitIndex, persistentState.snapshot.lastIncludedIndex)
                logger.trace("Updated commit index to snapshot's last included index \(persistentState.snapshot.lastIncludedIndex) based on majority")
                break
            }
        }

        // Apply newly committed entries to state machine
        applyCommittedEntries()
    }

    /// Applies the committed entries to the state machine.
    private func applyCommittedEntries() {
        while volatileState.lastApplied < volatileState.commitIndex {
            // Calculate the absolute index of the entry to apply
            let absoluteIndexToApply = volatileState.lastApplied + 1

            // Ensure the entry is not already covered by the snapshot
            if absoluteIndexToApply <= persistentState.snapshot.lastIncludedIndex {
                // This entry is part of the snapshot and should have been applied when the snapshot was installed.
                // We just increment lastApplied and continue.
                logger.trace("Skipping applying entry at absolute index \(absoluteIndexToApply) as it's part of the snapshot.")
                volatileState.lastApplied += 1
                continue
            }

            // Calculate the relative index in the current log array
            let relativeIndexInLog = absoluteIndexToApply - persistentState.snapshot.lastIncludedIndex - 1

            guard relativeIndexInLog >= 0, relativeIndexInLog < persistentState.log.count else {
                logger.error("Attempted to apply log entry at relative index \(relativeIndexInLog) (absolute: \(absoluteIndexToApply)) but it's out of bounds for current log (count: \(persistentState.log.count)). This indicates a bug.")
                break // Stop applying to prevent crashes
            }

            let entry = persistentState.log[relativeIndexInLog]

            if let key = entry.key {
                let oldValue = persistentState.stateMachine[key]
                persistentState.stateMachine[key] = entry.value
                logger.trace("Applied entry at absolute index \(absoluteIndexToApply): \(key) = \(entry.value ?? "nil") (was: \(oldValue ?? "nil"))")
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
    /// - Parameter lastLogIndex: The last log index of the other node.
    /// - Parameter lastLogTerm: The last log term of the other node.
    /// - Returns: True if the log is at least as up to date, false otherwise.
    private func isLogAtLeastAsUpToDate(lastLogIndex: Int, lastLogTerm: Int) -> Bool {
        // Calculate the absolute last log index and term for this node
        let myLastLogIndex = persistentState.log.count + persistentState.snapshot.lastIncludedIndex
        let myLastLogTerm = persistentState.log.last?.term ?? persistentState.snapshot.lastIncludedTerm

        if lastLogTerm > myLastLogTerm {
            return true
        }
        if lastLogTerm < myLastLogTerm {
            return false
        }
        // Terms are the same, compare by index
        return lastLogIndex >= myLastLogIndex
    }

    // MARK: - Snapshotting

    /// Loads the snapshot on startup.
    private func loadSnapshotOnStartup() async {
        do {
            if let snapshot = try await persistence.loadSnapshot(for: persistentState.ownPeer.id) {
                persistentState.snapshot = snapshot
                persistentState.currentTerm = snapshot.lastIncludedTerm
                persistentState.stateMachine = snapshot.stateMachine
                logger.info("Successfully loaded snapshot up to index \(snapshot.lastIncludedIndex) from disk.")
            }
        } catch {
            logger.error("Failed to load snapshot on startup: \(error)")
        }
    }

    /// Checks if a snapshot should be created.
    private func shouldCreateSnapshot() -> Bool {
        guard persistence.compactionThreshold > 0 else {
            return false
        }
        let entriesSinceLastSnapshot = volatileState.lastApplied - persistentState.snapshot.lastIncludedIndex
        return entriesSinceLastSnapshot >= persistence.compactionThreshold && !isSnapshotting
    }

    /// Creates a new snapshot.
    private func createSnapshot() async throws {
        guard !isSnapshotting else {
            logger.debug("Snapshotting already in progress, skipping new snapshot creation.")
            return
        }

        isSnapshotting = true
        defer { isSnapshotting = false }

        let snapshotLastIndex = volatileState.lastApplied
        // Ensure lastIncludedTerm is correct based on the actual log entry at that index
        let snapshotLastTerm: Int
        if snapshotLastIndex == 0 {
            snapshotLastTerm = 0 // Or some initial term for an empty log
        } else if snapshotLastIndex == persistentState.snapshot.lastIncludedIndex {
            // The snapshot is being taken at the same point as the previous one (no new entries applied beyond it)
            snapshotLastTerm = persistentState.snapshot.lastIncludedTerm
        } else {
            let relativeIndex = snapshotLastIndex - persistentState.snapshot.lastIncludedIndex - 1
            if relativeIndex >= 0, relativeIndex < persistentState.log.count {
                snapshotLastTerm = persistentState.log[relativeIndex].term
            } else {
                logger.error("Failed to determine term for snapshot at index \(snapshotLastIndex). Log state: \(persistentState.log.count), snapshot last included: \(persistentState.snapshot.lastIncludedIndex)")
                // Fallback, this indicates a potential issue in log or lastApplied management
                throw RaftError.snapshotCreationError("Could not determine term for snapshot last index.")
            }
        }

        let newSnapshot = Snapshot(
            lastIncludedIndex: snapshotLastIndex,
            lastIncludedTerm: snapshotLastTerm,
            stateMachine: persistentState.stateMachine,
        )

        try await persistence.saveSnapshot(newSnapshot, for: persistentState.ownPeer.id)
        persistentState.snapshot = newSnapshot

        // After snapshot is saved, truncate the log
        if snapshotLastIndex > persistentState.snapshot.lastIncludedIndex {
            let entriesToRemove = snapshotLastIndex - persistentState.snapshot.lastIncludedIndex
            if entriesToRemove > 0, entriesToRemove <= persistentState.log.count {
                persistentState.log.removeSubrange(0 ..< entriesToRemove)
                logger.info("Truncated log after snapshot, removed \(entriesToRemove) entries. New log length: \(persistentState.log.count)")
            } else {
                logger.warning("Attempted to truncate \(entriesToRemove) entries but log has \(persistentState.log.count) entries. No truncation performed.")
            }
        }
    }

    /// Sends a snapshot to a specific peer.
    ///
    /// - Parameter peer: The peer to send the snapshot to.
    private func sendSnapshotToPeer(_ peer: Peer) async throws {
        guard volatileState.state == .leader else { return } // Only leader sends snapshots

        let currentTerm = persistentState.currentTerm
        let leaderID = persistentState.ownPeer.id

        let snapshot = persistentState.snapshot

        logger.info("Sending InstallSnapshot RPC to \(peer.id) (snapshot last index: \(snapshot.lastIncludedIndex))")

        let request = InstallSnapshotRequest(
            term: currentTerm,
            leaderID: leaderID,
            snapshot: snapshot,
        )

        do {
            let response = try await transport.installSnapshot(request, on: peer, isolation: #isolation)

            if response.term > persistentState.currentTerm {
                logger.info("Received higher term \(response.term) during InstallSnapshot, becoming follower of \(peer.id)")
                becomeFollower(newTerm: response.term, currentLeaderId: peer.id)
                return
            }

            // Upon successful installation, update matchIndex and nextIndex for this peer
            leaderState.matchIndex[peer.id] = snapshot.lastIncludedIndex
            leaderState.nextIndex[peer.id] = snapshot.lastIncludedIndex + 1
            logger.info("Successfully sent snapshot to \(peer.id). Updated nextIndex to \(leaderState.nextIndex[peer.id] ?? 0) and matchIndex to \(leaderState.matchIndex[peer.id] ?? 0)")

        } catch {
            logger.error("Failed to send InstallSnapshot to \(peer.id): \(error)")
        }
    }

    // MARK: - State Transitions

    /// Becomes a follower.
    ///
    /// - Parameters:
    ///   - newTerm: The new term.
    ///   - currentLeaderId: The current leader ID.
    private func becomeFollower(newTerm: Int, currentLeaderId: Int?) {
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

    /// Becomes a candidate.
    private func becomeCandidate() {
        logger.info("Transitioning to candidate for term \(persistentState.currentTerm + 1)")
        volatileState.state = .candidate
        persistentState.currentTerm += 1
        persistentState.votedFor = persistentState.ownPeer.id
        volatileState.currentLeaderID = nil
    }

    /// Becomes a leader.
    private func becomeLeader() {
        logger.info("Transitioning to leader for term \(persistentState.currentTerm)")
        volatileState.state = .leader
        volatileState.currentLeaderID = persistentState.ownPeer.id

        // Initialize nextIndex and matchIndex for all peers
        let lastLogIndex = persistentState.log.count + persistentState.snapshot.lastIncludedIndex
        for peer in persistentState.peers {
            leaderState.nextIndex[peer.id] = lastLogIndex + 1 // Next log index to send to that server
            leaderState.matchIndex[peer.id] = 0 // Highest log entry known to be replicated on server
        }

        resetElectionTimer()
    }
}
