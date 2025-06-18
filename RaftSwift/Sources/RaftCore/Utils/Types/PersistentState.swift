/// The durable state persisted to disk across crashes
struct PersistentState: Sendable {
    /// Latest term server has seen (initialized to 0)
    var currentTerm: Int = 0

    /// Candidate ID that received vote in current term (or null)
    var votedFor: Int?

    /// Log entries, each containing a command for the state machine
    var log: [LogEntry] = []

    /// State machine state
    var stateMachine: [String: String] = [:]

    /// Latest snapshot of the state machine
    var snapshot: Snapshot = .init()

    /// The self peer config
    var ownPeer: Peer

    /// List of peers in the cluster
    var peers: [Peer]

    /// The configuration of the Raft node
    var config: RaftConfig

    /// Whether the node is currently snapshotting
    var isSnapshotting = false

    /// The persistence of the node
    var persistence: any RaftNodePersistence

    /// Initializes the persistent state
    /// - Parameters:
    ///   - ownPeer: The own peer
    ///   - peers: The list of peers
    ///   - config: The configuration of the Raft node
    ///   - persistence: The persistence layer
    init(
        ownPeer: Peer,
        peers: [Peer],
        config: RaftConfig,
        persistence: any RaftNodePersistence,
    ) {
        self.ownPeer = ownPeer
        self.peers = peers
        self.config = config
        self.persistence = persistence
    }
}

extension PersistentState {
    /// The length of the log
    ///
    /// Returns the last snapshot index plus the number of entries in the log.
    var logLength: Int {
        snapshot.lastIncludedIndex + log.count
    }
}
