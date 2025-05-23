/// A snapshot of the entire state machine
struct Snapshot {
    /// Index of last log entry included in the snapshot
    var lastIncludedIndex: Int = 0

    /// Term of last log entry included in the snapshot
    var lastIncludedTerm: Int = 0

    /// State machine state
    var stateMachine: [String: String] = [:]
}
