/// Protocol for a Raft node application
public protocol RaftNodeApplication: Sendable {
    /// The own peer
    var ownPeer: Peer { get }

    /// The list of peers
    var peers: [Peer] { get }

    /// Initializes the server
    /// - Parameters:
    ///   - ownPeer: The own peer
    ///   - peers: The list of peers
    init(ownPeer: Peer, peers: [Peer])

    /// Starts the node
    func serve() async throws
}
