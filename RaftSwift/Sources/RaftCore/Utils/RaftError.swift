/// Errors that can occur in the Raft node.
enum RaftError: Error {
    /// Thrown when a method is called on a node that is not the leader,
    /// but the method requires the node to be the leader.
    case notLeader

    /// Throwsn when a snapshot cannot be created.
    case snapshotCreationError(_ reason: String)
}
