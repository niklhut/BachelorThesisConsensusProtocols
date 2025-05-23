import GRPCCore
import GRPCNIOTransportHTTP2
import Logging
import RaftCore

/// A Raft client that uses gRPC for communication
public final class RaftGRPCClient: RaftClientApplication {
    /// The list of peers
    let peers: [Peer]

    /// The logger
    let logger = Logger(label: "raft.RaftGRPCClient")

    public init(peers: [Peer]) {
        self.peers = peers
    }

    public func runInteractiveClient() async throws {
        let client = RaftClient(
            peers: peers,
            transport: GRPCClientTransport(clientPool: GRPCClientPool())
        )
        let interactiveConsoleClient = InteractiveConsoleClient(client: client)

        try await interactiveConsoleClient.run()
    }

    public func runStressTest(operations: Int, concurrency: Int) async throws {
        let client = RaftClient(
            peers: peers,
            transport: GRPCClientTransport(clientPool: GRPCClientPool())
        )
        let stressTestClient = StressTestClient(client: client)

        try await stressTestClient.run(operations: operations, concurrency: concurrency)
    }
}
