/// Protocol for a Raft client application
public protocol RaftClientApplication: Sendable {
    /// Initializes the client
    /// - Parameters:
    ///   - peers: The list of peers
    init(peers: [Peer])

    /// Runs the interactive client
    /// - Throws: Any errors thrown by the client
    func runInteractiveClient() async throws

    /// Runs the stress test
    /// - Throws: Any errors thrown by the client
    func runStressTest(operations: Int, concurrency: Int) async throws
}
