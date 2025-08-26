/// Protocol for a Raft node application
public protocol RaftNodeApplication: Sendable {
    /// The own peer
    var ownPeer: Peer { get }

    /// The list of peers
    var peers: [Peer] { get }

    /// The persistence layer
    var persistence: any RaftNodePersistence { get }

    /// Whether to collect metrics
    var collectMetrics: Bool { get }

    /// Whether to use a manual lock
    var useManualLock: Bool { get }

    /// Initializes the server
    /// - Parameters:
    ///   - ownPeer: The own peer
    ///   - peers: The list of peers
    ///   - persistence: The persistence layer
    ///   - collectMetrics: Whether to collect metrics
    ///   - useManualLock: Whether to use a manual lock
    init(
        ownPeer: Peer,
        peers: [Peer],
        persistence: any RaftNodePersistence,
        collectMetrics: Bool,
        useManualLock: Bool,
    )

    /// Starts the node
    func serve() async throws
}
