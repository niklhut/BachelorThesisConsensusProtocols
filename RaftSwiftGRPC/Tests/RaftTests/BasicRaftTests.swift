import GRPCCore
import GRPCNIOTransportHTTP2
import Logging
@testable import Raft
import Testing

@Suite("Basic Raft Tests")
struct BasicRaftTests: Sendable {
    var peers = [Raft_Peer]()
    var servers: [GRPCServer<HTTP2ServerTransport.Posix>] = .init()
    var serverTasks: [Task<Void, Error>] = []

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
        for (index, server) in servers.enumerated() {
            serverTasks.append(Task {
                do {
                    try await server.serve()
                } catch {
                    print("Server finished serving")
                    await nodes[index].shutdown()
                }
            })
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

    /// Finds the index of the leader node.
    ///
    /// - Parameter excluding: The index of the node to exclude from the search.
    /// - Throws: An error if no leader is found.
    /// - Returns: The index of the leader node.
    private func findLeaderIndex(excluding: Int? = nil) async throws -> Int {
        let leader = try await findLeader(excluding: excluding)
        let index = peers.firstIndex { $0.id == leader.id }
        try #require(index != nil, "Leader not found in peers")
        return index!
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

    @Test("Leader failover")
    func testLeaderFailover() async throws {
        // Find the leader
        let originalLeaderIndex = try await findLeaderIndex()

        // Simulate leader crash
        serverTasks[originalLeaderIndex].cancel()

        print("Leader crashed")

        // Wait for new election
        try await Task.sleep(for: .seconds(1))

        // Find new leader
        let newLeaderIndex = try await findLeaderIndex(excluding: originalLeaderIndex)

        // Verify new leader is different from old leader
        #expect(newLeaderIndex != originalLeaderIndex, "A new leader should be elected")

        // Test the new leader can accept writes
        let putResponse = try await withClient(peer: peers[newLeaderIndex]) { client in
            try await client.put(.with { request in
                request.key = "afterFailover"
                request.value = "newLeaderValue"
            })
        }
        #expect(putResponse.success, "Put should succeed")

        // Wait for replication
        try await Task.sleep(for: .seconds(1))

        // Verify entry is replicated to all running nodes
        for i in 0 ..< peers.count where i != originalLeaderIndex {
            let getResponse = try await withClient(peer: peers[i]) { client in
                try await client.getDebug(.with { request in
                    request.key = "afterFailover"
                })
            }
            #expect(getResponse.value == "newLeaderValue", "Entry should be replicated to peer \(peers[i].id) after failover")
        }
    }

    @Test("Term propagation")
    mutating func testTermPropagation() async throws {
        // Find the current leader
        let leaderIndex = try await findLeaderIndex()

        // Force multiple elections to increase term
        for _ in 0 ..< 2 {
            // Shut down current leader to force a new election
            serverTasks[leaderIndex].cancel()
            serverTasks.remove(at: leaderIndex)
            peers.remove(at: leaderIndex)

            // Wait for a new leader to be elected
            try await Task.sleep(for: .seconds(2))

            // Find the new leader and verify it has a higher term
            _ = try await findLeader(excluding: leaderIndex)
        }

        // Check that term has propagated to all nodes
        let terms = try await withThrowingTaskGroup { group in
            // Collect term from each functioning node
            for i in 0 ..< peers.count {
                group.addTask { [self] in
                    let term = try await withClient(peer: peers[i]) { client in
                        try await client.getServerTerm(.init()).term
                    }
                    return (i, term)
                }
            }

            var results = [(Int, UInt64)]()
            for try await result in group {
                results.append(result)
            }
            return results
        }

        // Verify all terms are the same
        let firstTerm = terms.first?.1 ?? 0
        #expect(firstTerm > 2, "Term should be greater than 2 after multiple elections")
        for (_, term) in terms {
            #expect(term == firstTerm, "All nodes should have the same term")
        }
    }
}
