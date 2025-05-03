import Distributed
import DistributedCluster
import Foundation

extension DistributedReception.Key {
    static var raftNode: DistributedReception.Key<RaftNode> {
        "raftNode"
    }
}

distributed actor RaftNode: LifecycleWatch {
    typealias ActorSystem = ClusterSystem

    // MARK: - Properties

    private let config: RaftConfig

    private var state: RaftState = .follower
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

    private var majority: Int {
        (peers.count + 1) / 2
    }

    // Only used by leader
    var nextIndex: [ActorSystem.ActorID: Int] = [:]
    var matchIndex: [ActorSystem.ActorID: Int] = [:]

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

    // TODO: check if discardableResult is justified
    @discardableResult
    public distributed func appendEntries(
        term: Int,
        leaderId: ActorSystem.ActorID,
        prevLogIndex: Int,
        prevLogTerm: Int,
        entries: [LogEntry],
        leaderCommit: Int
    ) async throws -> AppendEntriesReturn {
        actorSystem.log.trace("Received append entries from \(leaderId)")
        lastHeartbeat = Date()

        // TODO: implement
        return .init(term: currentTerm, success: true)
    }

    struct RequestVoteReturn: Codable, Equatable {
        var term: Int
        var voteGranted: Bool
    }

    public distributed func requestVote(
        term: Int,
        candidateId: ActorSystem.ActorID,
        lastLogIndex: Int,
        lastLogTerm: Int
    ) async throws -> RequestVoteReturn {
        actorSystem.log.trace("Received request vote from \(candidateId)")
        lastHeartbeat = Date()

        if term < currentTerm {
            return .init(term: currentTerm, voteGranted: false)
        }

        if term > currentTerm {
            actorSystem.log.info("Received higher term, becoming follower")
            currentTerm = term
            votedFor = nil
            state = .follower
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

    public distributed func appendClientEntries(
        entries: [LogEntryValue]
    ) async throws {
        // TODO: implement
        return
    }

    // MARK: - Internal

    distributed func start() {
        findPeers()
        startTimer()
    }

    private distributed func startTimer() {
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

    private distributed func checkElectionTimeout() async throws {
        let now = Date()
        if now.timeIntervalSince(lastHeartbeat) * 1000 >= Double(electionTimeout) {
            actorSystem.log.info("Election timeout reached")
            try await self.startElection()
        }
    }

    private func startElection() async throws {
        actorSystem.log.trace("Starting election")
        currentTerm += 1
        state = .candidate
        votedFor = id

        // Reset election timeout
        electionTimeout = Int.random(in: config.electionTimeoutRange)
        lastHeartbeat = Date()

        try await requestVotes()
    }

    private func requestVotes() async throws {
        actorSystem.log.trace("Requesting votes, peers: \(peers)")

        let isElectedLeader = try await withThrowingTaskGroup(of: RequestVoteReturn.self) { group in
            var votes = 0

            for peer in peers {
                group.addTask {
                    guard peer.id != self.id else {
                        return await .init(term: self.currentTerm, voteGranted: true)
                    }
                    self.actorSystem.log.trace("Requesting vote from \(peer.id)")
                    let result = try await peer.requestVote(
                        term: self.currentTerm, candidateId: self.id, lastLogIndex: self.log.count,
                        lastLogTerm: self.log.last?.term ?? 0)
                    self.actorSystem.log.trace(
                        "Received vote from \(peer.id): \(result.voteGranted)")
                    return result
                }
            }

            for try await vote in group {
                if vote.term == currentTerm && vote.voteGranted {
                    votes += 1
                }

                if votes > majority {
                    return true
                }
            }
            return false
        }

        // Check if is candidate because node might recieve AppendEntries RPC 
        // from other server which is legitimate leader for current term, which 
        // invalidates this election.
        if isElectedLeader && state == .candidate {
            actorSystem.log.info("Received majority of votes, becoming leader")
            state = .leader
            startSendingHeartbeats()
        }
    }

    private func replicateLog(entries: [LogEntry]) async throws {
        // TODO: do we actually retry on the snapshotted data or do we retry on the current data?
        let currentTermSnapshot = currentTerm
        let prevLogIndexSnapshot = log.count
        let prevLogTermSnapshot = log.last?.term ?? 0
        let commitIndexSnapshot = commitIndex

        try await withThrowingTaskGroup(of: (id: ActorSystem.ActorID, result: AppendEntriesReturn).self) { group in
            var successCount = 0

            for peer in peers {
                group.addTask {
                    guard peer.id != self.id else {
                        return await (id: self.id, result: .init(term: self.currentTerm, success: true))
                    }
                    self.actorSystem.log.trace("Requesting vote from \(peer.id)")
                    let result = try await peer.appendEntries(
                        term: currentTermSnapshot,
                        leaderId: self.id,
                        prevLogIndex: prevLogIndexSnapshot,
                        prevLogTerm: prevLogTermSnapshot,
                        entries: entries,
                        leaderCommit: commitIndexSnapshot)
                    self.actorSystem.log.trace("Received append entries response from \(peer.id)")
                    return (id: peer.id, result: result)
                }
            }
    
            for try await (id, result) in group {
                if result.term > currentTerm {
                    actorSystem.log.info("Received higher term, becoming follower")
                    currentTerm = result.term
                    votedFor = nil
                    state = .follower
                }

                if result.success {
                    matchIndex[id] = prevLogIndexSnapshot + entries.count
                    nextIndex[id] = matchIndex[id]! + 1
                    successCount += 1
                }

                if successCount > majority {
                    commitIndex += entries.count
                    // TODO: apply log entries
                    return
                }
            }
        }
    }

    private func startSendingHeartbeats() {
        guard state == .leader else {
            actorSystem.log.warning("Tried to start heartbeat task in non-leader state")
            return
        }

        // TODO: cancel this task when no longer leader
        let task = Task {
            while !Task.isCancelled {
                do {
                    try await self.sendHeartbeats()
                    // TODO: make sure to update last heartbeat also when normal append entries are sent
                    if lastHeartbeat.timeIntervalSinceNow * 1000 < Double(config.heartbeatInterval) {
                        lastHeartbeat = Date()
                        try await Task.sleep(for: .milliseconds(config.heartbeatInterval))
                    }
                } catch {
                    actorSystem.log.error("Error in heartbeat task: \(error)")
                }
            }
        }

        self.heartbeatTask = task
    }

    private func sendHeartbeats() async throws {
        actorSystem.log.trace("Sending heartbeats")
        // TODO: this is not done, I should call a separate append entries function 
        // which makes sure the entries are actually commited or retransmitted.
        try await peers.concurrentForEach { @Sendable [self] peer in
            guard peer.id != id else {
                return
            }
            try await peer.appendEntries(
                term: currentTerm, leaderId: id, prevLogIndex: log.count,
                prevLogTerm: log.last?.term ?? 0, entries: [], leaderCommit: commitIndex)
        }
    }

    private func isLogAtLeastAsUpToDate(lastLogIndex: Int, lastLogTerm: Int) -> Bool {
        let localLastLogTerm = self.log.last?.term ?? 0
        let localLastLogIndex = self.log.count

        if lastLogTerm != localLastLogTerm {
            return lastLogTerm > localLastLogTerm
        }

        return lastLogIndex >= localLastLogIndex
    }

    // MARK: - Lifecycle

    func findPeers() {
        guard listingTask == nil else {
            actorSystem.log.warning("Already looking for peers")
            return
        }

        listingTask = Task {
            for await peer in await actorSystem.receptionist.listing(of: .raftNode) {
                actorSystem.log.info("Found peer: \(peer)")
                peers.insert(peer)
                nextIndex[peer.id] = log.count
                matchIndex[peer.id] = 0
                watchTermination(of: peer)
            }
        }
    }

    func terminated(actor id: DistributedCluster.ActorID) async {
        let _ = peers.remove(
            peers.first(where: { node in
                node.id == id
            })!)
        actorSystem.log.warning("Peer \(id) terminated")
    }

    deinit {
        listingTask?.cancel()
    }
}
