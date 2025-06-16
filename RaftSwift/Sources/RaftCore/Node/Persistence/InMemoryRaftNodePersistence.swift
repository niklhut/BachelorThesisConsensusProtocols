/// In memory implementation of node persistence.
///
/// This implementation of persistence is not persistent accos node failures, since the snapshots are saved in memory.
public actor InMemoryRaftNodePersistence: RaftNodePersistence {
    var snapshots: [Int: Snapshot] = [:]

    public let compactionThreshold: Int

    public init(compactionThreshold: Int) {
        self.compactionThreshold = compactionThreshold
    }

    public func saveSnapshot(_ snapshot: Snapshot, for nodeId: Int) async throws {
        snapshots[nodeId] = snapshot
    }

    public func loadSnapshot(for nodeId: Int) async throws -> Snapshot? {
        snapshots[nodeId]
    }

    public func deleteSnapshot(for nodeId: Int) async throws {
        snapshots[nodeId] = nil
    }
}
