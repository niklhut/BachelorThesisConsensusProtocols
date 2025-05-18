import GRPCCore
import GRPCNIOTransportHTTP2
import Logging
@testable import Raft
import Testing

@Suite("Basic Raft Tests")
struct BasicRaftTests: Sendable {
    var peers = [Raft_Peer]()
    var servers: [GRPCServer<HTTP2ServerTransport.Posix>] = .init()

    let logger = Logger(label: "raft.BasicRaftTests")

    // MARK: - Lifecycle

    init() async throws {
        // Use randomized base port to allow parallel test execution
        let basePort = Int.random(in: 10000 ... 20000)

        // First create the peers
        for i in 1 ... 5 {
            peers.append(Raft_Peer(id: UInt32(i), address: "0.0.0.0", port: UInt32(basePort + i)))
        }

        // Then create the nodes and servers
        var nodes = [RaftNode]()
        for peer in peers {
            let node = RaftNode(peer, config: RaftConfig(), peers: peers.filter { $0.id != peer.id })
            nodes.append(node)
            let peerService = PeerService(node: node)
            let clientService = ClientService(node: node)

            let server = GRPCServer(
                transport: .http2NIOPosix(
                    address: .ipv4(host: peer.address, port: Int(peer.port)),
                    transportSecurity: .plaintext
                ),
                services: [peerService, clientService]
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

    // deinit {
    //     // Shutdown all servers
    //     for server in servers {
    //         server.beginGracefulShutdown()
    //     }
    // }

    // MARK: - Helpers

    /// Finds the leader node.
    ///
    /// - Parameter excluding: The index of the node to exclude from the search.
    /// - Throws: An error if no leader is found.
    /// - Returns: The leader node.
    private func findLeader(excluding: Int? = nil) async throws -> Raft_Peer {
        var leader: Raft_Peer?

        for (index, peer) in peers.enumerated() where excluding != index {
            let response = try await withClient(peer: peer) { client in
                try await client.getServerState(.init())
            }
            if response.state == .leader {
                leader = peer
                break
            }
        }

        try #require(leader != nil, "No leader found")
        return leader!
    }

    /// Executes a block of code with a gRPC client for a specific peer.
    ///
    /// - Parameters:
    ///   - peer: The peer to execute the block for.
    ///   - body: The block to execute.
    /// - Throws: Any errors thrown by the block.
    /// - Returns: The result of the block.
    private func withClient<T: Sendable>(peer: Raft_Peer, _ body: @Sendable @escaping (_ client: Raft_RaftClient.Client<HTTP2ClientTransport.Posix>) async throws -> T) async throws -> T {
        try await withGRPCClient(
            transport: .http2NIOPosix(
                target: peer.target,
                transportSecurity: .plaintext
            ),
        ) { client in
            let peerClient = Raft_RaftClient.Client(wrapping: client)
            return try await body(peerClient)
        }
    }

    // MARK: - Tests

    @Test("Leader election")
    func testLeaderElection() async throws {
        // Verify there is exactly one leader
        let leaders = try await withThrowingTaskGroup(of: Bool.self) { group in
            for peer in peers {
                group.addTask { [self] in
                    try await withClient(peer: peer) { client in
                        try await client.getServerState(.init()).state == .leader
                    }
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

    @Test("Log replication")
    func testLogReplication() async throws {
        // Find the leader
        let leader = try await findLeader()

        // Append entries to the leader
        let putResponse = try await withClient(peer: leader) { client in
            try await client.put(.with { request in
                request.key = "testKey"
                request.value = "testValue"
            })
        }
        #expect(putResponse.success, "Put should succeed")

        // Wait for replication
        try await Task.sleep(for: .seconds(1))

        // Verify all nodes have the entry
        for peer in peers {
            let getResponse = try await withClient(peer: peer) { client in
                try await client.getDebug(.with { request in
                    request.key = "testKey"
                })
            }
            #expect(getResponse.value == "testValue", "Entry should be replicated to all nodes")
        }
    }
}
