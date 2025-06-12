/// Leader to Follower, used for installing snapshots
public struct InstallSnapshotRequest: Sendable, Codable {
    /// The term of the leader
    public let term: Int
    /// The ID of the leader
    public let leaderID: Int
    /// The snapshot to install
    public let snapshot: Snapshot

    /// Creates a new InstallSnapshotRequest
    /// - Parameters:
    ///   - term: The term of the leader
    ///   - leaderID: The ID of the leader
    ///   - snapshot: The snapshot to install
    public init(term: Int, leaderID: Int, snapshot: Snapshot) {
        self.term = term
        self.leaderID = leaderID
        self.snapshot = snapshot
    }
}

/// Follower to Leader, response to InstallSnapshotRequest
public struct InstallSnapshotResponse: Sendable, Codable {
    /// The term of the follower
    public let term: Int

    /// Creates a new InstallSnapshotResponse
    /// - Parameters:
    ///   - term: The term of the follower
    public init(term: Int) {
        self.term = term
    }
}
