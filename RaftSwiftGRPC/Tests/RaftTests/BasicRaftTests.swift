import GRPCCore
import GRPCNIOTransportHTTP2
import Logging
@testable import Raft
import Testing

@Suite("Basic Raft Tests")
final class BasicRaftTests {
    var servers: [GRPCServer<HTTP2ServerTransport.Posix>] = []
    var nodes: [RaftNode] = []

    let logger = Logger(label: "raft.BasicRaftTests")

    // MARK: - Lifecycle

    init() async throws {
        // Use randomized base port to allow parallel test execution
        let basePort = Int.random(in: 10000 ... 20000)

        // First create the peers
        var peers: [Raft_Peer] = []
        for i in 1 ... 5 {
            peers.append(Raft_Peer(id: UInt32(i), address: "0.0.0.0", port: UInt32(basePort + i)))
        }

        // Then create the nodes and servers
        for peer in peers {
            let node = RaftNode(peer, config: RaftConfig(), peers: peers.filter { $0.id != peer.id })
            nodes.append(node)
            let peerService = PeerService(node: node)
            let adminService = AdminService(node: node)

            let server = GRPCServer(
                transport: .http2NIOPosix(
                    address: .ipv4(host: peer.address, port: Int(peer.port)),
                    transportSecurity: .plaintext
                ),
                services: [peerService, adminService]
            )
            servers.append(server)
        }

        // Start all servers concurrently but don't wait for them to finish serving
        Task.detached { [servers] in
            try await withThrowingTaskGroup(of: Void.self) { group in
                for server in servers {
                    group.addTask {
                        try await server.serve()
                    }
                }

                try await group.waitForAll()
            }
        }

        // Wait for all servers to start listening
        for (server, node) in zip(servers, nodes) {
            if let address = try await server.listeningAddress {
                logger.info("Server listening on \(address)")
            }

            logger.info("Starting node")
            await node.start()
            logger.info("Node started")
        }

        logger.info("All nodes started")

        try await Task.sleep(for: .seconds(1))
    }

    deinit {
        // Shutdown all servers
        for server in servers {
            server.beginGracefulShutdown()
        }
        servers = []
        nodes = []
    }

    // MARK: - Helpers

    /// Finds the leader node.
    ///
    /// - Parameter excluding: The index of the node to exclude from the search.
    /// - Throws: An error if no leader is found.
    /// - Returns: The leader node.
    private func findLeader(excluding: Int? = nil) async throws -> RaftNode {
        var leader: RaftNode?

        for (index, node) in nodes.enumerated() where excluding != index {
            if try await node.getState().state == .leader {
                leader = node
                break
            }
        }

        try #require(leader != nil, "No leader found")
        return leader!
    }

    // MARK: - Tests

    @Test("Leader election")
    func testLeaderElection() async throws {
        // Verify there is exactly one leader
        let leaders = try await withThrowingTaskGroup(of: Bool.self) { group in
            for node in nodes {
                group.addTask {
                    try await node.getState().state == .leader
                }
            }

            var leaderCount = 0
            for try await isLeader in group {
                if isLeader { leaderCount += 1 }
            }
            return leaderCount
        }

        #expect(leaders == 1, "There should be exactly one leader")
    }
}
