/// Protocol for a Raft node application
public protocol RaftNodeApplication: Sendable {
    /// The own peer
    var ownPeer: Peer { get }

    /// The list of peers
    var peers: [Peer] { get }

    /// The persistence layer
    var persistence: any RaftNodePersistence { get }

    /// Whether to use a manual lock
    var useManualLock: Bool { get }

    /// Initializes the server
    /// - Parameters:
    ///   - ownPeer: The own peer
    ///   - peers: The list of peers
    ///   - persistence: The persistence layer
    init(ownPeer: Peer, peers: [Peer], persistence: any RaftNodePersistence, useManualLock: Bool)

    /// Starts the node
    func serve() async throws
}
