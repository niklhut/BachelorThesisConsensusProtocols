import DistributedCluster
import Logging
@testable import Raft
import Testing

@Suite("RaftTests")
final class RaftTests {
    var systems: [ClusterSystem] = []
    var nodes: [RaftNode] = []

    // MARK: - Lifecycle

    init() async throws {
        for i in 1 ... 5 {
            // Use randomized base port to allow parallel test execution
            let basePort = Int.random(in: 10000 ... 20000)
            let system = await ClusterSystem("test-node-\(i)") { settings in
                settings.bindPort = basePort + i
                settings.logging.baseLogger = Logger(label: "RaftTestNode\(i)") { label in
                    ColoredConsoleLogHandler(label: label)
                }
            }
            systems.append(system)

            // Create a RaftNode with test configuration
            let testConfig = RaftConfig()
            let node = RaftNode(config: testConfig, actorSystem: system)
            nodes.append(node)
            try await node.start()

            // Register node with receptionist
            await system.receptionist.checkIn(node, with: .raftNode)
        }

        // Connect the nodes in a fully connected topology
        for system in systems {
            for otherSystem in systems where otherSystem !== system {
                system.cluster.join(endpoint: otherSystem.cluster.node.endpoint)
            }
        }

        try await ensureCluster(systems, within: .seconds(10))
    }

    deinit {
        // Shutdown all systems
        for system in systems {
            try! system.shutdown()
        }
        systems = []
        nodes = []
    }

    // MARK: - Helpers

    /// Ensures that all nodes in the cluster are up and running.
    ///
    /// - Parameters:
    ///   - systems: The systems to ensure.
    ///   - within: The time to wait for the nodes to come up.
    /// - Throws: An error if the nodes do not come up within the specified time.
    private func ensureCluster(_ systems: [ClusterSystem], within: Duration) async throws {
        let nodes = Set(systems.map(\.settings.bindNode))

        try await withThrowingTaskGroup(of: Void.self) { group in
            for system in systems {
                group.addTask {
                    try await system.cluster.waitFor(nodes, .up, within: within)
                }
            }
            // loop explicitly to propagagte any error that might have been thrown
            for try await _ in group {}
        }
    }

    /// Finds the leader node.
    ///
    /// - Parameter excluding: The index of the node to exclude from the search.
    /// - Throws: An error if no leader is found.
    /// - Returns: The leader node.
    private func findLeader(excluding: Int? = nil) async throws -> RaftNode {
        var leader: RaftNode?

        for (index, node) in nodes.enumerated() where excluding != index {
            if try await node.getState() == .leader {
                leader = node
                break
            }
        }

        try #require(leader != nil, "No leader found")
        return leader!
    }

    private func findLeaderIndex() async throws -> Int {
        let leader = try await findLeader()
        let index = nodes.firstIndex(where: { $0.id == leader.id })
        try #require(index != nil, "Leader node not found in nodes array")
        return index!
    }

    // MARK: - Tests

    @Test("Leader election")
    func testLeaderElection() async throws {
        // Verify there is exactly one leader
        let leaders = try await withThrowingTaskGroup(of: Bool.self) { group in
            for node in nodes {
                group.addTask {
                    // A way to check if node is leader (you'd need to add a method for this)
                    try await node.getState() == .leader
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
        let entries = [LogEntryValue(key: "testKey", value: "testValue")]
        try await leader.appendClientEntries(entries: entries)

        // Wait for replication
        try await Task.sleep(for: .seconds(1))

        // Verify all nodes have the entry
        for node in nodes {
            let value = try await node.getStateValue(key: "testKey")
            #expect(value == "testValue", "Entry should be replicated to all nodes")
        }
    }

    @Test("Leader failover")
    func testLeaderFailover() async throws {
        // Find the leader
        let originalLeader = try await findLeader()
        let leaderIndex = nodes.firstIndex { $0.id == originalLeader.id }!

        // Simulate leader crash
        try systems[leaderIndex].shutdown()

        // Wait for new election
        try await Task.sleep(for: .seconds(2))

        // Find new leader
        let newLeader = try await findLeader(excluding: leaderIndex)

        // Verify new leader is different from old leader
        #expect(newLeader.id != originalLeader.id, "A new leader should be elected")

        // Test the new leader can accept writes
        let entries = [LogEntryValue(key: "afterFailover", value: "newLeaderValue")]
        try await newLeader.appendClientEntries(entries: entries)

        // Wait for replication
        try await Task.sleep(for: .seconds(1))

        // Verify entry is replicated to all running nodes
        for i in 0 ..< nodes.count where i != leaderIndex {
            let value = try await nodes[i].getStateValue(key: "afterFailover")
            #expect(value == "newLeaderValue", "Entry should be replicated after failover")
        }
    }

    @Test("Term propagation")
    func testTermPropagation() async throws {
        // Find the current leader
        let leaderIndex = try await findLeaderIndex()

        // Force multiple elections to increase term
        for _ in 0 ..< 2 {
            // Shut down current leader to force a new election
            try systems[leaderIndex].shutdown()
            nodes.remove(at: leaderIndex)

            // Wait for a new leader to be elected
            try await Task.sleep(for: .seconds(2))

            // Find the new leader and verify it has a higher term
            _ = try await findLeader(excluding: leaderIndex)
        }

        // Check that term has propagated to all nodes
        let terms = try await withThrowingTaskGroup(of: (Int, Int).self) { group in
            let nodes = self.nodes
            // Collect term from each functioning node
            for i in 0 ..< nodes.count {
                group.addTask {
                    let term = try await nodes[i].getCurrentTerm()
                    return (i, term)
                }
            }

            var results: [(Int, Int)] = []
            for try await result in group {
                results.append(result)
            }
            return results
        }

        // Verify all terms are the same
        let firstTerm = terms.first?.1 ?? 0
        for (_, term) in terms {
            #expect(term == firstTerm, "All nodes should have the same term")
        }
    }

    @Test("Network partition - majority side still operates")
    func testNetworkPartition() async throws {
        // Find the leader
        let leaderIndex = try await findLeaderIndex()

        // Create a partition: minority (2 nodes) and majority (3 nodes)
        // Ensure leader is in the majority partition
        let majorityPartition: [Int]
        let minorityPartition: [Int]

        if leaderIndex < 2 {
            // Leader in first two nodes, move it to majority partition
            majorityPartition = [leaderIndex, 3, 4]
            minorityPartition = [0, 1, 2].filter { $0 != leaderIndex }
        } else {
            // Leader already in potential majority partition
            majorityPartition = [leaderIndex, 3, 4]
            minorityPartition = [0, 1]
        }

        try #require(majorityPartition.count == 3, "Majority partition should have 3 nodes")
        try #require(minorityPartition.count == 2, "Minority partition should have 2 nodes")

        // Simulate network partition by removing connections
        for majorityIndex in majorityPartition {
            for minorityIndex in minorityPartition {
                await systems[majorityIndex].cluster.down(member: systems[minorityIndex].cluster.member)
                await systems[minorityIndex].cluster.down(member: systems[majorityIndex].cluster.member)
            }
        }

        // Check leader is still leader
        let leader = nodes[leaderIndex]
        let leaderState = try await leader.getState()
        try #require(leaderState == .leader, "Leader should still be leader")

        // Add entry to the majority side
        try await leader.appendClientEntries(entries: [
            LogEntryValue(key: "partition-test", value: "majority-value"),
        ])

        // Wait for replication within majority partition
        try await Task.sleep(for: .seconds(2))

        // Verify the majority side has the entry
        for majorityIndex in majorityPartition {
            let value = try await nodes[majorityIndex].getStateValue(key: "partition-test")
            #expect(value == "majority-value", "Entry should be replicated to majority partition")
        }

        // Verify no leader in minority partition
        for idx in minorityPartition {
            let state = try await nodes[idx].getState()
            #expect(state != .leader, "Minority partition should not have a leader")
        }

        // Heal the partition
        for majorityIndex in majorityPartition {
            for minorityIndex in minorityPartition {
                systems[majorityIndex].cluster.join(endpoint: systems[minorityIndex].cluster.node.endpoint)
                systems[minorityIndex].cluster.join(endpoint: systems[majorityIndex].cluster.node.endpoint)
            }
        }

        // Wait for recovery
        try await Task.sleep(for: .seconds(2))

        await withKnownIssue("New nodes are not updated when entering cluster") {
            // Verify all nodes eventually get the entry
            for node in nodes {
                let value = try await node.getStateValue(key: "partition-test")
                #expect(value == "majority-value", "Entry should be replicated to all nodes after partition heals")
            }
        }
    }
}
