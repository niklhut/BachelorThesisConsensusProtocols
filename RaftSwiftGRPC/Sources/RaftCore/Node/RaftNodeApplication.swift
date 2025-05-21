/// Protocol for a Raft node application
public protocol RaftNodeApplication: Sendable {
    /// Initializes the server
    /// - Parameters:
    ///   - id: The ID of this server
    ///   - port: The port to listen on for incoming connections
    ///   - peers: The list of peers
    init(id: Int, port: Int, peers: [Peer])

    /// Starts the node
    func serve() async throws
}
