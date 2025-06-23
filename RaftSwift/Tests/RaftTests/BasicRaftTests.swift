import GRPCCore
import GRPCNIOTransportHTTP2
import Logging
@testable import RaftApp
@testable import RaftCore
@testable import RaftGRPCTransport
import Testing

@Suite("Basic Raft Tests")
struct BasicRaftTests: Sendable {
    var peers = [RaftCore.Peer]()
    var servers = [GRPCServer<HTTP2ServerTransport.Posix>]()
    var interceptors = [NetworkPartitionInterceptor]()
    var serverTasks: [Task<Void, Error>] = []

    let logger = Logger(label: "raft.BasicRaftTests")

    var majority: Int {
        peers.count / 2 + 1
    }

    // MARK: - Lifecycle

    init() async throws {
        // Use randomized base port to allow parallel test execution
        let basePort = Int.random(in: 10000 ... 20000)

        // First create the peers
        for i in 1 ... 5 {
            peers.append(RaftCore.Peer(id: i, address: "0.0.0.0", port: basePort + i))
        }

        // Then create the nodes and servers
        var nodes = [RaftNode]()
        for peer in peers {
            let node = RaftNode(
                peer,
                peers: peers.filter { $0.id != peer.id }, config: RaftConfig(), transport: GRPCNodeTransport(clientPool: GRPCClientPool(interceptors: [
                    ServerIDInjectionInterceptor(peerID: peer.id),
                ])),
            )
            nodes.append(node)
            let interceptor = NetworkPartitionInterceptor(logger: logger)
            interceptors.append(interceptor)
            let peerService = PeerService(node: node)
            let clientService = ClientService(node: node)

            let server = GRPCServer(
                transport: .http2NIOPosix(
                    address: .ipv4(host: peer.address, port: Int(peer.port)),
                    transportSecurity: .plaintext,
                ),
                services: [peerService, clientService],
                interceptors: [interceptor],
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

        try await Task.sleep(for: .seconds(5))
    }

    // MARK: - Helpers

    /// Finds the leader node.
    ///
    /// - Parameter excluding: The index of the node to exclude from the search.
    /// - Throws: An error if no leader is found.
    /// - Returns: The leader node.
    private func findLeader(excluding: Int? = nil) async throws -> RaftCore.Peer {
        var leader: RaftCore.Peer?

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
    private func withClient<T: Sendable>(peer: RaftCore.Peer, _ body: @Sendable @escaping (_ client: Raft_RaftClient.Client<HTTP2ClientTransport.Posix>) async throws -> T) async throws -> T {
        try await withGRPCClient(
            transport: .http2NIOPosix(
                target: peer.target,
                transportSecurity: .plaintext,
            ),
        ) { client in
            let peerClient = Raft_RaftClient.Client(wrapping: client)
            return try await body(peerClient)
        }
    }

    /// Partitions an array into two groups based on the provided indices.
    ///
    /// - Parameters:
    ///   - array: The array to partition.
    ///   - groupOneSize: The size of the first group.
    ///   - ensureInGroupOne: The indices of elements that must be in the first group.
    /// - Throws: An error if the inputs are invalid.
    /// - Returns: A tuple containing the two groups.
    func partition(
        _ array: [some Any],
        groupOneSize: Int,
        ensureInGroupOne indices: Int...,
    ) throws -> (groupOneIndices: [Int], groupTwoIndices: [Int]) {
        // Validate inputs
        for index in indices {
            try #require(index >= 0 && index < array.count, "Index \(index) out of bounds")
        }

        try #require(groupOneSize > 0 && groupOneSize <= array.count, "Group one size must be between 1 and \(array.count)")

        // Check if we can accommodate all required elements in group one
        try #require(indices.count <= groupOneSize, "Cannot ensure \(indices.count) elements in group one when group one size is \(groupOneSize)")

        // Create a set of indices to ensure fast lookups
        let ensuredIndices = Set(indices)

        // Get all available indices and separate them
        let allIndices = Array(0 ..< array.count)
        var remainingIndices = allIndices.filter { !ensuredIndices.contains($0) }

        // Shuffle the remaining indices to ensure random distribution
        remainingIndices.shuffle()

        // Calculate how many more indices we need for group one
        let additionalIndicesNeeded = groupOneSize - ensuredIndices.count

        // Create the groups of indices
        var groupOneIndices = Array(ensuredIndices)

        // Add additional indices to group one if needed
        if additionalIndicesNeeded > 0 {
            groupOneIndices.append(contentsOf: remainingIndices.prefix(additionalIndicesNeeded))
        }

        // The rest go to group two
        let groupTwoIndices = Array(remainingIndices.suffix(remainingIndices.count - additionalIndicesNeeded))

        return (groupOneIndices, groupTwoIndices)
    }

    // MARK: - Tests

    @Test("Leader election")
    func leaderElection() async throws {
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
    func logReplication() async throws {
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
    func leaderFailover() async throws {
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
    mutating func termPropagation() async throws {
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

    @Test("Network partition - majority side still operates")
    mutating func networkPartition() async throws {
        // Find the leader
        let leaderIndex = try await findLeaderIndex()

        let (majorityPartition, minorityPartition) = try partition(peers, groupOneSize: majority, ensureInGroupOne: leaderIndex)

        logger.info("Creating network partition - Majority: \(majorityPartition.map { peers[$0].id }), Minority: \(minorityPartition.map { peers[$0].id })")

        try #require(majorityPartition.count == majority, "Majority partition should have \(majority) nodes")
        try #require(minorityPartition.count == peers.count - majority, "Minority partition should have \(peers.count - majority) nodes")

        for majorityPeerIndex in majorityPartition {
            await interceptors[majorityPeerIndex].blockPeers(minorityPartition.map { peers[$0].id })
        }
        for minorityPeerIndex in minorityPartition {
            await interceptors[minorityPeerIndex].blockPeers(majorityPartition.map { peers[$0].id })
        }

        // Check leader is still leader
        let leaderState = try await withClient(peer: peers[leaderIndex]) { client in
            try await client.getServerState(.init()).state
        }
        try #require(leaderState == .leader, "Leader should still be leader")

        // Add entry to the majority side
        let putResponse = try await withClient(peer: peers[leaderIndex]) { client in
            try await client.put(.with { request in
                request.key = "partition-test"
                request.value = "majority-value"
            })
        }
        try #require(putResponse.success, "Put should succeed")

        // Wait for replication within majority partition
        try await Task.sleep(for: .seconds(1))

        // Verify the majority side has the entry
        for majorityIndex in majorityPartition {
            let value = try await withClient(peer: peers[majorityIndex]) { client in
                try await client.getDebug(.with { request in
                    request.key = "partition-test"
                })
            }
            #expect(value.value == "majority-value", "Entry should be replicated to majority partition")
        }

        // Verify no leader in minority partition
        for idx in minorityPartition {
            let state = try await withClient(peer: peers[idx]) { client in
                try await client.getServerState(.init()).state
            }
            #expect(state != .leader, "Minority partition should not have a leader")
        }

        // Heal the partition
        for interceptorIndex in interceptors.indices {
            await interceptors[interceptorIndex].clearBlockedPeers()
        }

        // Wait for recovery
        try await Task.sleep(for: .seconds(2))

        // Verify all nodes eventually get the entry
        for peerIndex in peers.indices {
            let value = try await withClient(peer: peers[peerIndex]) { client in
                try await client.getDebug(.with { request in
                    request.key = "partition-test"
                })
            }
            #expect(value.value == "majority-value", "Entry should be replicated to all node \(peers[peerIndex].id) after partition heals")
        }
    }

    @Test("Leader hint")
    func testLeaderHint() async throws {
        // Find the leader
        let leader = try await findLeader()

        for peer in peers where peer.id != leader.id {
            let putResponse = try await withClient(peer: peer) { client in
                try await client.put(.with { request in
                    request.key = "testKey"
                    request.value = "testValue"
                })
            }
            #expect(!putResponse.success, "Put should fail")
            #expect(putResponse.leaderHint == leader.toGRPC(), "Put response leader hint should be the leader for peer \(peer.id)")

            let getResponse = try await withClient(peer: peer) { client in
                try await client.get(.with { request in
                    request.key = "testKey"
                })
            }
            #expect(!getResponse.hasValue, "Get should fail")
            #expect(getResponse.leaderHint == leader.toGRPC(), "Get response leader hint should be the leader for peer \(peer.id)")
        }
    }
}
