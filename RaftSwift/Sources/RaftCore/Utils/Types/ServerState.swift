/// The current role of the server in the Raft cluster
public enum ServerState: Sendable, Codable {
    /// Passive role; waits for messages from leader
    case follower

    /// Candidate trying to gather votes to become leader
    case candidate

    /// Active leader role that sends heartbeats and manages log replication
    case leader
}
