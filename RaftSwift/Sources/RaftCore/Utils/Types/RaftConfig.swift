/// Configuration for the Raft node.
public struct RaftConfig: Sendable {
    /// The range of election timeout in milliseconds
    public let electionTimeoutRange: ClosedRange<Int> = 2000 ... 2500

    /// The interval of heartbeats in milliseconds
    public let heartbeatInterval = 50

    /// Creates a new configuration
    public init() {}
}
