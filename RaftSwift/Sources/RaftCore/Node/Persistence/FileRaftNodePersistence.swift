import Foundation

/// File based implementation of node persistence.
public actor FileRaftNodePersistence: RaftNodePersistence {
    private let directoryURL: URL
    public let compactionThreshold: Int

    public init(compactionThreshold: Int) throws {
        self.compactionThreshold = compactionThreshold
        let fileManager = FileManager.default
        let path = "raft-snapshots"
        let url = URL(fileURLWithPath: path, isDirectory: true)

        if url.path.hasPrefix("/") {
            // Absolute path
            directoryURL = url
        } else {
            // Relative path
            directoryURL = URL(fileURLWithPath: fileManager.currentDirectoryPath)
                .appendingPathComponent(path, isDirectory: true)
        }
        // Ensure the directory exists
        try fileManager.createDirectory(at: directoryURL, withIntermediateDirectories: true)
    }

    private func fileURL(for nodeId: Int) -> URL {
        directoryURL.appendingPathComponent("snapshot_\(nodeId).json")
    }

    public func saveSnapshot(_ snapshot: Snapshot, for nodeId: Int) async throws {
        let url = fileURL(for: nodeId)
        let data = try JSONEncoder().encode(snapshot)

        // Perform the blocking file write operation on a detached task
        // to prevent blocking the actor's executor.
        try await Task.detached {
            try data.write(to: url, options: .atomic)
        }.value

        print("Saved snapshot to \(url.path)")
    }

    public func loadSnapshot(for nodeId: Int) async throws -> Snapshot? {
        let url = fileURL(for: nodeId)
        guard FileManager.default.fileExists(atPath: url.path) else {
            return nil
        }

        // Perform the blocking file read operation on a detached task
        let data = try await Task.detached {
            try Data(contentsOf: url)
        }.value

        return try JSONDecoder().decode(Snapshot.self, from: data)
    }

    public func deleteSnapshot(for nodeId: Int) async throws {
        let url = fileURL(for: nodeId)

        // Perform the blocking file delete operation on a detached task
        try await Task.detached {
            if FileManager.default.fileExists(atPath: url.path) {
                try FileManager.default.removeItem(at: url)
            }
        }.value
    }
}
