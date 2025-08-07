/// Configuration for the Raft node.
public struct RaftConfig: Sendable {
    /// The range of election timeout in milliseconds
    public let electionTimeoutRange: ClosedRange<Int> = 500 ... 1000

    /// The interval of heartbeats in milliseconds
    public let heartbeatInterval = 50

    /// The threshold of log entries to compact
    public let compactionThreshold = 1000

    /// Creates a new configuration
    public init() {}
}
