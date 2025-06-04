import RaftCore

/// Protocol for a Raft client application
public protocol RaftTestApplication: Sendable {
    /// Initializes the client
    /// - Parameters:
    ///   - peers: The list of peers
    ///   - machineName: The name of the machine on which the test is running
    init(peers: [Peer], machineName: String)

    /// Runs the interactive client
    /// - Throws: Any errors thrown by the client
    func runInteractiveClient() async throws

    /// Runs the stress test
    /// - Throws: Any errors thrown by the client
    func runStressTest(operations: Int, concurrency: Int) async throws

    /// Runs the functionality tests
    /// - Throws: Any errors thrown by the client or test suite
    func runFunctionalityTests() async throws
}
