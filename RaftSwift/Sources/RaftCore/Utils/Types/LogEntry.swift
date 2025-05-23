/// A single entry in the replicated log
public struct LogEntry: Sendable, Codable, Hashable {
    /// Term when the entry was received by the leader
    public let term: Int

    /// Key for the key-value operation, optional for no-op entries
    public let key: String?

    /// Optional value (null if this is a deletion or no-op)
    public let value: String?

    /// Creates a new LogEntry
    /// - Parameters:
    ///   - term: Term when the entry was received by the leader
    ///   - key: Key for the key-value operation, optional for no-op entries
    ///   - value: Optional value (null if this is a deletion or no-op)
    public init(
        term: Int,
        key: String?,
        value: String?
    ) {
        self.term = term
        self.key = key
        self.value = value
    }
}
