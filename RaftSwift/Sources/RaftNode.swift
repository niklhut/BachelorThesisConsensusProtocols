import Foundation
import Distributed
import DistributedCluster

distributed actor RaftNode {
    typealias ActorSystem = ClusterSystem

    // MARK: - Properties

    let config: RaftConfig
    var state: RaftState = .candidate
    var currentTerm = 0
    var votedFor: Int?
    var log: [LogEntry] = []
    var commitIndex = 0
    var lastApplied = 0

    // Only used by leader
    var lastHeartbeat = Date()
    var nextIndex: [Int: Int] = [:]
    var matchIndex: [Int: Int] = [:]

    init(config: RaftConfig = .init(), actorSystem: ActorSystem) {
        self.config = config
        self.actorSystem = actorSystem
    }

    // MARK: - Server RPCs

    struct AppendEntriesReturn: Codable, Equatable {
        var term: Int
        var success: Bool
    }

    public distributed func appendEntries(
        term: Int,
        leaderId: Int,
        prevLogIndex: Int,
        prevLogTerm: Int,
        entries: [LogEntry],
        leaderCommit: Int
    ) async throws -> AppendEntriesReturn {
        // TODO: implement
        return .init(term: 0, success: false)
    }

    struct RequestVoteReturn: Codable, Equatable {
        var term: Int
        var voteGranted: Bool
    }

    public distributed func requestVote(
        term: Int,
        candidateId: Int,
        lastLogIndex: Int,
        lastLogTerm: Int
    ) async throws -> RequestVoteReturn {
        // TODO: implement
        return .init(term: 0, voteGranted: false)
    }

    // MARK: - Client RPCs

    public distributed func appendClientEntries(
        entries: [LogEntryValue]
    ) async throws {
        // TODO: implement
        return
    }

    // MARK - Internal
}
