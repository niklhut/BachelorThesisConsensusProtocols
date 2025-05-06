import Distributed
import DistributedCluster
import Foundation

extension DistributedReception.Key {
    static var raftNode: DistributedReception.Key<RaftNode> {
        "raftNode"
    }
}

distributed actor RaftNode: LifecycleWatch {
    // MARK: - Properties

    private let config: RaftConfig

    private var state: RaftState = .follower {
        didSet {
            if state != .leader {
                if let heartbeatTask {
                    heartbeatTask.cancel()
                    self.heartbeatTask = nil
                }

                // Clear leader-specific state when transitioning away from leader
                self.nextIndex.removeAll()
                self.matchIndex.removeAll()
            } else if state == .leader {
                currentLeaderId = id

                // Initialize leader state when becoming leader
                for peer in peers {
                    nextIndex[peer.id] = log.count + 1
                    matchIndex[peer.id] = 0
                }
            }
        }
    }
    private var currentTerm: Int = 0
    private var votedFor: ActorSystem.ActorID?
    private var log: [LogEntry] = []
    private var commitIndex = 0
    private var lastApplied = 0

    /// Election timeout in milliseconds
    private var electionTimeout: Int
    private var lastHeartbeat = Date()
    private var timerTask: Task<Void, Never>?
    private var heartbeatTask: Task<Void, Never>?
    private var listingTask: Task<Void, Never>?
    private var peers: Set<RaftNode> = []
    private var stateMachine: [String: String] = [:]
    private var currentLeaderId: ActorSystem.ActorID?

    private var majority: Int {
        peers.count / 2 + 1
    }

    // Only used by leader
    var nextIndex: [ActorSystem.ActorID: Int] = [:]
    var matchIndex: [ActorSystem.ActorID: Int] = [:]

    /// Initializes the node.
    ///
    /// - Parameters:
    ///   - config: The configuration for the node.
    ///   - actorSystem: The actor system for the node.
    init(config: RaftConfig = .init(), actorSystem: ActorSystem) {
        self.config = config
        self.actorSystem = actorSystem
        self.electionTimeout = Int.random(in: config.electionTimeoutRange)
    }

    // MARK: - Server RPCs

    struct AppendEntriesReturn: Codable, Equatable {
        var term: Int
        var success: Bool
    }

    /// Handles an append entries RPC.
    ///
    /// - Parameters:
    ///   - term: The term of the leader.
    ///   - leaderId: The ID of the leader.
    ///   - prevLogIndex: The index of the previous log entry.
    ///   - prevLogTerm: The term of the previous log entry.
    ///   - entries: The log entries to append.
    ///   - leaderCommit: The index of the last log entry committed by the leader.
    /// - Returns: The node's term and success of the append entries RPC.
    public distributed func appendEntries(
        term: Int,
        leaderId: ActorSystem.ActorID,
        prevLogIndex: Int,
        prevLogTerm: Int,
        entries: [LogEntry],
        leaderCommit: Int
    ) async -> AppendEntriesReturn {
        actorSystem.log.trace("Received append entries from \(leaderId)")

        if term < currentTerm {
            return AppendEntriesReturn(term: currentTerm, success: false)
        }

        if term > currentTerm {
            actorSystem.log.info("Received higher term, becoming follower")
            await becomeFollower(newTerm: term, currentLeaderId: leaderId)
        }

        resetElectionTimer()

        // First, update commit index
        if leaderCommit > commitIndex {
            commitIndex = min(leaderCommit, log.count)

            applyCommittedEntries()
        }

        // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if prevLogIndex > 0 {
            if log.count < prevLogIndex {
                // Log is too short
                actorSystem.log.info("Log is too short")
                return AppendEntriesReturn(term: currentTerm, success: false)
            }

            if log[prevLogIndex - 1].term != prevLogTerm {
                // Term mismatch at the expected previous index
                actorSystem.log.info("Term mismatch at the expected previous index")
                return AppendEntriesReturn(term: currentTerm, success: false)
            }
        }

        // If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it
        // Append any new entries not already in the log
        if entries.count > 0 {
            let newEntries = entries
            var conflict = false
            var conflictIndex = 0

            for i in 0..<newEntries.count {
                let entryIndex = prevLogIndex + i + 1

                if entryIndex <= log.count {
                    // This is an existing entry in our log - check for conflicts
                    if log[entryIndex - 1].term != newEntries[i].term {
                        // Conflict found - different term for same index
                        actorSystem.log.info("Conflict found - different term for same index")
                        conflict = true
                        conflictIndex = i
                        break
                    }
                    // If terms match, this entry is already replicated correctly
                } else {
                    // We've reached the end of our log - remaining entries are new
                    break
                }
            }

            if conflict {
                // Remove conflicting entry and everything that follows
                let deleteFromIndex = prevLogIndex + conflictIndex
                if deleteFromIndex <= log.count {
                    log.removeSubrange(deleteFromIndex - 1..<log.count)
                }

                // Append the remaining entries from the conflict point
                log.append(contentsOf: newEntries[conflictIndex...])
            } else {
                // No conflicts, append any entries that aren't already in our log
                let newEntriesStartIndex = max(0, log.count - prevLogIndex)
                if newEntriesStartIndex < newEntries.count {
                    log.append(contentsOf: newEntries[newEntriesStartIndex...])
                }
            }
        }

        return AppendEntriesReturn(term: currentTerm, success: true)
    }

    struct RequestVoteReturn: Codable, Equatable {
        var term: Int
        var voteGranted: Bool
    }

    /// Handles a request vote RPC.
    ///
    /// - Parameters:
    ///   - term: The term of the candidate.
    ///   - candidateId: The ID of the candidate.
    ///   - lastLogIndex: The index of the candidate's last log entry.
    ///   - lastLogTerm: The term of the candidate's last log entry.
    /// - Returns: The node's term and whether the vote was granted.
    public distributed func requestVote(
        term: Int,
        candidateId: ActorSystem.ActorID,
        lastLogIndex: Int,
        lastLogTerm: Int
    ) async -> RequestVoteReturn {
        actorSystem.log.trace("Received request vote from \(candidateId)")
        resetElectionTimer()

        if term < currentTerm {
            return .init(term: currentTerm, voteGranted: false)
        }

        if term > currentTerm {
            actorSystem.log.info("Received higher term, becoming follower")
            await becomeFollower(newTerm: term, currentLeaderId: candidateId)
        }

        let canGrantVote =
            (votedFor == nil || votedFor == candidateId)
            && isLogAtLeastAsUpToDate(lastLogIndex: lastLogIndex, lastLogTerm: lastLogTerm)

        if canGrantVote {
            votedFor = candidateId
            return .init(term: currentTerm, voteGranted: true)
        }

        return .init(term: currentTerm, voteGranted: false)
    }

    // MARK: - Client RPCs

    /// Handles an append client entries RPC.
    ///
    /// - Parameters:
    ///   - entries: The log entries to append.
    /// - Throws: RaftError.notLeader if the node is not the leader.
    public distributed func appendClientEntries(
        entries: [LogEntryValue]
    ) async throws {
        guard state == .leader else {
            throw RaftError.notLeader(leaderId: currentLeaderId)
        }
        await replicateLog(entries: [LogEntry(term: currentTerm, data: entries)])
    }

    /// Handles a get state value RPC.
    ///
    /// - Parameter key: The key to get the value for.
    /// - Returns: The value for the given key.
    /// - Throws: RaftError.notLeader if the node is not the leader.
    public distributed func getStateValue(
        key: String
    ) async throws -> String? {
        guard state == .leader else {
            throw RaftError.notLeader(leaderId: currentLeaderId)
        }
        return stateMachine[key]
    }

    // MARK: - Internal

    /// Starts the node.
    distributed func start() {
        findPeers()
        startTimer()
    }

    /// Starts the timer task.
    private distributed func startTimer() {
        // Cancel existing timer if any
        timerTask?.cancel()

        let task = Task {
            while !Task.isCancelled {
                do {
                    try await self.checkElectionTimeout()
                    try await Task.sleep(for: .milliseconds(100))
                } catch {
                    actorSystem.log.error("Error in timer task: \(error)")
                }
            }
        }

        self.timerTask = task
    }

    /// Checks if the election timeout has been reached.
    private distributed func checkElectionTimeout() async throws {
        let now = Date()
        if now.timeIntervalSince(lastHeartbeat) * 1000 >= Double(electionTimeout) {
            actorSystem.log.info("Election timeout reached")
            try await self.startElection()
        }
    }

    /// Starts an election.
    private distributed func startElection() async throws {
        actorSystem.log.trace("Starting election")
        currentTerm += 1
        state = .candidate
        votedFor = id
        currentLeaderId = nil

        // Reset election timeout
        electionTimeout = Int.random(in: config.electionTimeoutRange)
        resetElectionTimer()

        try await requestVotes()
    }

    /// Requests votes from all peers.
    private distributed func requestVotes() async throws {
        actorSystem.log.trace("Requesting votes, peers: \(peers)")

        var votes = 1  // Count own vote
        let requiredVotes = majority

        let currentTermSnapshot = currentTerm

        try await withThrowingTaskGroup(
            of: (peerId: ActorSystem.ActorID, vote: RequestVoteReturn).self
        ) { group in
            for peer in peers where peer.id != self.id {
                group.addTask {
                    self.actorSystem.log.trace("Requesting vote from \(peer.id)")
                    let result = try await peer.requestVote(
                        term: currentTermSnapshot,
                        candidateId: self.id,
                        lastLogIndex: self.log.count,
                        lastLogTerm: self.log.last?.term ?? 0
                    )
                    return (peer.id, result)
                }
            }

            for try await (peerId, vote) in group {
                if Task.isCancelled {
                    break
                }

                self.actorSystem.log.trace("Received vote from \(peerId): \(vote.voteGranted)")

                // Check if the peer has a higher term
                if vote.term > currentTermSnapshot {
                    actorSystem.log.info("Received higher term, becoming follower")
                    await becomeFollower(newTerm: vote.term, currentLeaderId: peerId)
                    return
                }

                // Count votes only if we're still a candidate and in the same term
                if state == .candidate && vote.term == currentTermSnapshot && vote.voteGranted {
                    votes += 1

                    if votes >= requiredVotes {
                        actorSystem.log.info(
                            "Received majority of votes (\(votes)/\(peers.count)), becoming leader"
                        )
                        state = .leader
                        startSendingHeartbeats()

                        // Cancel remaining vote collection tasks
                        group.cancelAll()
                        return
                    }
                }
            }

            if state == .candidate {
                actorSystem.log.info(
                    "Election failed, received \(votes)/\(requiredVotes) votes")
            }
        }
    }

    /// Replicates log entries to all peers.
    ///
    /// - Parameter entries: The log entries to replicate.
    private func replicateLog(entries: [LogEntry]) async {
        guard state == .leader else {
            actorSystem.log.error("Node is not leader, cannot replicate log")
            return
        }

        let currentTermSnapshot = currentTerm
        let prevLogIndexSnapshot = log.count
        let prevLogTermSnapshot = log.last?.term ?? 0
        let commitIndexSnapshot = commitIndex

        // Add log entries to log
        log.append(contentsOf: entries)

        // Create tracker with leader pre-marked as successful
        let tracker = ReplicationTracker(peerCount: peers.count, majority: majority)
        await tracker.markSuccess(id: id)
        resetElectionTimer()

        // Start a background task for replication coordination
        Task {
            await withTaskGroup { group in
                // Start individual replication tasks for each peer
                for peer in peers where peer.id != id {
                    group.addTask {
                        await self.replicateToPeer(
                            peer: peer,
                            tracker: tracker,
                            currentTermSnapshot: currentTermSnapshot,
                            prevLogIndexSnapshot: prevLogIndexSnapshot,
                            prevLogTermSnapshot: prevLogTermSnapshot,
                            commitIndexSnapshot: commitIndexSnapshot,
                            entries: entries
                        )
                    }
                }
            }

            // Wait for majority to replicate successfully
            await tracker.waitForMajority()

            // Once majority is achieved, update commit index and apply entries
            if state == .leader && currentTerm == currentTermSnapshot {
                await updateCommitIndexAndApply(
                    entries: entries,
                    prevLogIndexSnapshot: prevLogIndexSnapshot)
            }
        }

        // Wait for majority replication before returning
        await tracker.waitForMajority()
    }

    /// Replicates log entries to a single peer.
    ///
    /// - Parameters:
    ///   - peer: The peer to replicate to.
    ///   - tracker: The tracker to use for tracking replication progress.
    ///   - currentTermSnapshot: The current term snapshot.
    ///   - prevLogIndexSnapshot: The previous log index snapshot.
    ///   - prevLogTermSnapshot: The previous log term snapshot.
    ///   - commitIndexSnapshot: The commit index snapshot.
    ///   - entries: The log entries to replicate.
    private func replicateToPeer(
        peer: RaftNode,
        tracker: ReplicationTracker,
        currentTermSnapshot: Int,
        prevLogIndexSnapshot: Int,
        prevLogTermSnapshot: Int,
        commitIndexSnapshot: Int,
        entries: [LogEntry]
    ) async {
        var retryCount = 0

        // Continue trying until successful or no longer leader
        while !Task.isCancelled && state == .leader && currentTerm == currentTermSnapshot
            && peers.contains(peer)
        {
            // Check if already successful (another task might have succeeded)
            if await tracker.isSuccessful(id: peer.id) {
                return
            }

            do {
                let peerNextIndex = nextIndex[peer.id] ?? prevLogIndexSnapshot + 1
                let peerPrevLogIndex = peerNextIndex - 1
                let peerPrevLogTerm =
                    peerPrevLogIndex <= 0
                    ? 0
                    : (peerPrevLogIndex <= log.count ? log[peerPrevLogIndex - 1].term : 0)

                // Calculate entries to send
                let entriesToSend: [LogEntry]
                if peerNextIndex <= prevLogIndexSnapshot {
                    // Need to send some previous entries
                    let startIndex = max(0, peerNextIndex - 1)
                    let previousEntries = Array(log[startIndex..<prevLogIndexSnapshot])
                    entriesToSend = previousEntries + entries
                } else {
                    entriesToSend = entries
                }

                actorSystem.log.trace("Sending \(entriesToSend.count) entries to \(peer.id)")
                let result = try await peer.appendEntries(
                    term: currentTermSnapshot,
                    leaderId: id,
                    prevLogIndex: peerPrevLogIndex,
                    prevLogTerm: peerPrevLogTerm,
                    entries: entriesToSend,
                    leaderCommit: commitIndexSnapshot)

                if Task.isCancelled {
                    return
                }

                if result.term > currentTerm {
                    actorSystem.log.info(
                        "Received higher term (\(result.term)), becoming follower")
                    await becomeFollower(newTerm: result.term, currentLeaderId: peer.id)
                    return
                }

                if result.success {
                    // Successful replication
                    let newMatchIndex = peerPrevLogIndex + entriesToSend.count
                    self.matchIndex[peer.id] = newMatchIndex
                    self.nextIndex[peer.id] = newMatchIndex + 1

                    await tracker.markSuccess(id: peer.id)
                    return
                } else {
                    // Log inconsistency, decrement nextIndex and retry
                    self.actorSystem.log.trace(
                        "Append entries failed for \(peer.id), retrying with earlier index")
                    self.nextIndex[peer.id] = max(1, (self.nextIndex[peer.id] ?? 1) - 1)
                    retryCount += 1

                    // Wait a bit before retrying with exponential backoff
                    try await Task.sleep(for: .milliseconds(50 * UInt64(min(16, 1 << retryCount))))
                }
            } catch {
                self.actorSystem.log.error("Error replicating to \(peer.id): \(error)")
                retryCount += 1
                // Wait before retrying with exponential backoff
                try? await Task.sleep(
                    for: .milliseconds(100 * UInt64(min(16, 1 << retryCount))))
            }
        }
    }

    /// Updates the commit index and applies the committed entries to the state machine.
    ///
    /// - Parameters:
    ///   - entries: The log entries to apply.
    ///   - prevLogIndexSnapshot: The previous log index snapshot.
    private func updateCommitIndexAndApply(
        entries: [LogEntry],
        prevLogIndexSnapshot: Int
    ) async {
        // Add own match index (implicitly the end of the log)
        var allMatchIndices = self.matchIndex
        allMatchIndices[self.id] = self.log.count

        // Calculate new commit index based on majority match indices
        let sortedIndices = Array(allMatchIndices.values).sorted()
        let majorityIndex = sortedIndices[self.majority - 1]

        // TODO: we could improve this by going backwards and checking if the term is the same, this would save us from going through all entries
        // Only update commit index if it's in the current term
        // (Raft safety requirement: only commit entries from current term)
        if self.commitIndex < majorityIndex {
            for i in self.commitIndex + 1...majorityIndex {
                if i <= self.log.count && self.log[i - 1].term == self.currentTerm {
                    self.commitIndex = i
                }
            }
        }

        // Apply newly committed entries to state machine
        self.applyCommittedEntries()
    }

    /// Applies the committed entries to the state machine.
    private func applyCommittedEntries() {
        while self.lastApplied < self.commitIndex {
            self.lastApplied += 1
            let entry = self.log[self.lastApplied - 1]
            for value in entry.data {
                self.stateMachine[value.key] = value.value
            }
        }
    }

    /// Let the node become a follower.
    ///
    /// - Parameters:
    ///   - newTerm: The new term.
    ///   - currentLeaderId: The ID of the current leader.
    private func becomeFollower(newTerm: Int, currentLeaderId: ActorSystem.ActorID) async {
        self.currentTerm = newTerm
        self.votedFor = nil
        self.state = .follower
        self.currentLeaderId = currentLeaderId
    }

    /// Starts sending heartbeats to all followers.
    private func startSendingHeartbeats() {
        guard state == .leader else {
            actorSystem.log.warning("Tried to start heartbeat task in non-leader state")
            return
        }

        // Cancel existing heartbeat task if any
        self.heartbeatTask?.cancel()

        let task = Task {
            while !Task.isCancelled {
                do {
                    if lastHeartbeat.timeIntervalSinceNow * 1000 < Double(config.heartbeatInterval)
                    {
                        await self.sendHeartbeats()
                    }

                    try await Task.sleep(for: .milliseconds(config.heartbeatInterval))
                } catch {
                    actorSystem.log.error("Error in heartbeat task: \(error)")
                }
            }
        }

        self.heartbeatTask = task
    }

    /// Sends heartbeats to all followers.
    private func sendHeartbeats() async {
        actorSystem.log.trace("Sending heartbeats")
        await replicateLog(entries: [])
    }

    /// Checks if the log is at least as up to date as the given log.
    ///
    /// - Parameters:
    ///   - lastLogIndex: The index of other node's last log entry.
    ///   - lastLogTerm: The term of other node's last log entry.
    private func isLogAtLeastAsUpToDate(lastLogIndex: Int, lastLogTerm: Int) -> Bool {
        let localLastLogTerm = self.log.last?.term ?? 0
        let localLastLogIndex = self.log.count

        if lastLogTerm != localLastLogTerm {
            return lastLogTerm > localLastLogTerm
        }

        return lastLogIndex >= localLastLogIndex
    }

    /// Resets the election timer.
    func resetElectionTimer() {
        lastHeartbeat = Date()
    }

    // MARK: - Lifecycle

    /// Continuously finds all peers in the cluster.
    func findPeers() {
        guard listingTask == nil else {
            actorSystem.log.warning("Already looking for peers")
            return
        }

        listingTask = Task {
            for await peer in await actorSystem.receptionist.listing(of: .raftNode) {
                actorSystem.log.info("Found peer: \(peer)")
                peers.insert(peer)
                watchTermination(of: peer)
            }
        }
    }

    func terminated(actor id: DistributedCluster.ActorID) async {
        // Remove from leader state tracking
        nextIndex.removeValue(forKey: id)
        matchIndex.removeValue(forKey: id)

        if let peerToRemove = peers.first(where: { $0.id == id }) {
            peers.remove(peerToRemove)
            actorSystem.log.warning("Peer \(id) terminated")
            // TODO: actually this should not happen, because now we stop 
            // replicating to an expected peer, which should
            // happen indefinitely.
            // We should only remove a node on the raft reconfiguration.
        }
    }

    deinit {
        listingTask?.cancel()
        timerTask?.cancel()
        heartbeatTask?.cancel()
    }
}
