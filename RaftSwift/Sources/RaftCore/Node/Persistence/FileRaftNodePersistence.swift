import Foundation

/// File based implementation of node persistence.
public actor FileRaftNodePersistence: RaftNodePersistence {
    private let directoryURL: URL
    public let compactionThreshold: Int

    public init(compactionThreshold: Int) throws {
        self.compactionThreshold = compactionThreshold
        let fileManager = FileManager.default
        let path = "raft-snapshots"

        // Relative path
        directoryURL = URL(fileURLWithPath: fileManager.currentDirectoryPath)
            .appendingPathComponent(path, isDirectory: true)

        // Ensure the directory exists
        try fileManager.createDirectory(at: directoryURL, withIntermediateDirectories: true)
    }

    private func fileURL(for nodeId: Int) -> URL {
        directoryURL.appendingPathComponent("snapshot_\(nodeId).json")
    }

    public func saveSnapshot(_ snapshot: Snapshot, for nodeId: Int) async throws {
        let url = fileURL(for: nodeId)
        let data = try JSONEncoder().encode(snapshot)

        try data.write(to: url, options: .atomic)

        print("Saved snapshot to \(url.path)")
    }

    public func loadSnapshot(for nodeId: Int) async throws -> Snapshot? {
        let url = fileURL(for: nodeId)
        guard FileManager.default.fileExists(atPath: url.path) else {
            return nil
        }

        let data = try Data(contentsOf: url)

        return try JSONDecoder().decode(Snapshot.self, from: data)
    }

    public func deleteSnapshot(for nodeId: Int) async throws {
        let url = fileURL(for: nodeId)

        if FileManager.default.fileExists(atPath: url.path) {
            try FileManager.default.removeItem(at: url)
        }
    }
}
