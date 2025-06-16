/// A snapshot of the entire state machine
public struct Snapshot: Sendable, Codable {
    /// Index of last log entry included in the snapshot
    public var lastIncludedIndex: Int = 0

    /// Term of last log entry included in the snapshot
    public var lastIncludedTerm: Int = 0

    /// State machine state
    public var stateMachine: [String: String] = [:]

    /// Initializes a new empty instance of the Snapshot struct.
    public init() {}

    /// Initializes a new instance of the Snapshot struct.
    /// - Parameters:
    ///   - lastIncludedIndex: The index of the last log entry included in the snapshot.
    ///   - lastIncludedTerm: The term of the last log entry included in the snapshot.
    ///   - stateMachine: The state machine state.
    public init(
        lastIncludedIndex: Int,
        lastIncludedTerm: Int,
        stateMachine: [String: String]
    ) {
        self.lastIncludedIndex = lastIncludedIndex
        self.lastIncludedTerm = lastIncludedTerm
        self.stateMachine = stateMachine
    }
}
