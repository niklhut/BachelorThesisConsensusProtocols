import DistributedCluster
@testable import Raft
import Testing

@Suite("RaftTests")
final class RaftTests {
    var systems: [ClusterSystem] = []
    var nodes: [RaftNode] = []

    init() async throws {
        for i in 1 ... 5 {
            // Use randomized base port to allow parallel test execution
            let basePort = Int.random(in: 10000 ... 20000)
            let system = await ClusterSystem("test-node-\(i)") { settings in
                settings.bindPort = basePort + i
            }
            systems.append(system)

            // Create a RaftNode with test configuration
            let testConfig = RaftConfig()
            let node = RaftNode(config: testConfig, actorSystem: system)
            nodes.append(node)

            // Register node with receptionist
            await system.receptionist.checkIn(node, with: .raftNode)
        }

        // Connect the nodes in a fully connected topology
        for system in systems {
            for otherSystem in systems where otherSystem !== system {
                system.cluster.join(endpoint: otherSystem.cluster.node.endpoint)
            }
        }

        // Wait for cluster formation
        try await Task.sleep(for: .milliseconds(500))
    }

    deinit {
        // Shutdown all systems
        for system in systems {
            try! system.shutdown()
        }
        systems = []
        nodes = []
    }

    @Test("Leader election")
    func testLeaderElection() async throws {
        // Start all nodes
        for node in nodes {
            try await node.start()
        }

        // Wait for election timeout
        try await Task.sleep(for: .seconds(2))

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
        // Start all nodes
        for node in nodes {
            try await node.start()
        }

        // Wait for a leader to be elected
        try await Task.sleep(for: .seconds(2))

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
        // Start all nodes
        for node in nodes {
            try await node.start()
        }

        // Wait for a leader to be elected
        try await Task.sleep(for: .seconds(2))

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

    // Helper to find the current leader
    private func findLeader(excluding: Int? = nil) async throws -> RaftNode {
        var leader: RaftNode?

        for (index, node) in nodes.enumerated() {
            if excluding == index { continue }
            if try await node.getState() == .leader {
                leader = node
                break
            }
        }

        try #require(leader != nil, "No leader found")
        return leader!
    }
}
