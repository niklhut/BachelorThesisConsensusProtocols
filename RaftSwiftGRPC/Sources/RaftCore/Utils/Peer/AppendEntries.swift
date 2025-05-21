/// Leader to Follower, used for log replication and heartbeats
public struct AppendEntriesRequest: Sendable, Codable {
    /// Leader's current term
    public let term: Int

    /// ID of the leader making the request
    public let leaderID: Int

    /// Index of log entry immediately preceding new ones
    public let prevLogIndex: Int

    /// Term of prevLogIndex entry
    public let prevLogTerm: Int

    /// Log entries to store
    public let entries: [LogEntry]

    /// Leader's commit index
    public let leaderCommit: Int

    /// Creates a new AppendEntriesRequest
    /// - Parameters:
    ///   - term: Leader's current term
    ///   - leaderID: ID of the leader making the request
    ///   - prevLogIndex: Index of log entry immediately preceding new ones
    ///   - prevLogTerm: Term of prevLogIndex entry
    ///   - entries: Log entries to store
    ///   - leaderCommit: Leader's commit index
    public init(
        term: Int,
        leaderID: Int,
        prevLogIndex: Int,
        prevLogTerm: Int,
        entries: [LogEntry],
        leaderCommit: Int
    ) {
        self.term = term
        self.leaderID = leaderID
        self.prevLogIndex = prevLogIndex
        self.prevLogTerm = prevLogTerm
        self.entries = entries
        self.leaderCommit = leaderCommit
    }
}

/// Follower to Leader, response to AppendEntriesRequest
public struct AppendEntriesResponse: Sendable, Codable {
    /// Current term, for leader to update itself
    public let term: Int

    /// True if follower contained entry matching prevLogIndex and prevLogTerm
    public let success: Bool

    /// Creates a new AppendEntriesResponse
    /// - Parameters:
    ///   - term: Current term of the follower
    ///   - success: True if follower contained entry matching prevLogIndex and prevLogTerm
    public init(
        term: Int,
        success: Bool
    ) {
        self.term = term
        self.success = success
    }
}
