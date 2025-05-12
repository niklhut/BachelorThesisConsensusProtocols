import DistributedCluster

struct AppendEntriesReturn: Codable, Equatable {
    var term: Int
    var success: Bool
}

struct RequestVoteReturn: Codable, Equatable {
    var term: Int
    var voteGranted: Bool
}

protocol RaftNodeRPC: DistributedActor, Hashable {
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
    func appendEntries(
        term: Int,
        leaderId: ID,
        prevLogIndex: Int,
        prevLogTerm: Int,
        entries: [LogEntry],
        leaderCommit: Int
    ) async throws -> AppendEntriesReturn

    /// Handles a request vote RPC.
    ///
    /// - Parameters:
    ///   - term: The term of the candidate.
    ///   - candidateId: The ID of the candidate.
    ///   - lastLogIndex: The index of the candidate's last log entry.
    ///   - lastLogTerm: The term of the candidate's last log entry.
    /// - Returns: The node's term and whether the vote was granted.
    func requestVote(
        term: Int,
        candidateId: ID,
        lastLogIndex: Int,
        lastLogTerm: Int
    ) async throws -> RequestVoteReturn
}
