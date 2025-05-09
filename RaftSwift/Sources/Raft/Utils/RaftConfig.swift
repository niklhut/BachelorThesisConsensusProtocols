struct RaftConfig {
    /// The range of election timeout in milliseconds
    let electionTimeoutRange: ClosedRange<Int> = 150 ... 300

    /// The interval of heartbeat in milliseconds
    let heartbeatInterval = 50

    /// The threshold of log entries to compact
    let compactionThreshold = 1000
}
