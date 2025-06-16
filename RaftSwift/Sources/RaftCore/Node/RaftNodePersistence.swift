/// Protocol to persist snapshots
public protocol RaftNodePersistence: Sendable {
    /// The compaction threshold
    ///
    /// This is the number of entries that are stored in memory before a snapshot is taken.
    var compactionThreshold: Int { get }

    /// Initializes a new instance of the RaftNodePersistence class.
    ///
    /// - Parameter compactionThreshold: The compaction threshold.
    init(compactionThreshold: Int) throws

    /// Saves a snapshot for a node.
    /// - Parameters:
    ///   - snapshot: The snapshot to save.
    ///   - nodeId: The id of the node to which the snapshot belongs.
    func saveSnapshot(_ snapshot: Snapshot, for nodeId: Int) async throws

    /// Loads a snapshot.
    /// - Parameter nodeId: The node id to which the snapshot belongs.
    /// - Returns: The loaded snapshot, if found.
    func loadSnapshot(for nodeId: Int) async throws -> Snapshot?

    /// Deletes a snapshot.
    /// - Parameter nodeId: The node id to which the snapshot belongs.
    func deleteSnapshot(for nodeId: Int) async throws
}
