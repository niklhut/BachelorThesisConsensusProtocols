import Foundation
import Logging
import RaftCore

/// Test client for Raft consensus protocol
public actor RaftTestClient<Transport: RaftPartitionTransport> {
    /// The Raft client to use for communication with the server.
    let client: RaftClient<Transport>

    /// The logger to use for logging.
    let logger: Logger

    /// Network partition controller for partition tests and to simulate node failure
    let partitionController: NetworkPartitionController<Transport>

    /// Test configuration
    public struct Config {
        public let electionTimeout: Duration
        public let replicationTimeout: Duration
        public let maxRetries: Int

        public init(
            electionTimeout: Duration = .seconds(2),
            replicationTimeout: Duration = .seconds(2),
            maxRetries: Int = 3
        ) {
            self.electionTimeout = electionTimeout
            self.replicationTimeout = replicationTimeout
            self.maxRetries = maxRetries
        }
    }

    let config: Config

    /// Initializes a new instance of the RaftTestClient class.
    /// - Parameters:
    ///   - client: The Raft client to use for communication with the server.
    ///   - partitionController: Optional controller for network partition simulation.
    ///   - config: Test configuration parameters.
    public init(
        client: RaftClient<Transport>,
        config: Config = Config()
    ) {
        self.client = client
        self.config = config
        logger = Logger(label: "raft.RaftTestClient")
        partitionController = NetworkPartitionController(peers: client.peers, client: client)
    }

    // MARK: - Test Suite Runner

    /// Runs a complete test suite
    /// - Returns: Test suite results
    public func runTestSuite() async -> RaftTestSuiteResult {
        let startTime = Date()
        var results: [RaftTestResult] = []

        // Basic tests
        await results.append(runTest("Leader Election", test: testLeaderElection))
        await results.append(runTest("Log Replication", test: testLogReplication))
        await results.append(runTest("Leader Failover", test: testLeaderFailover))
        await results.append(runTest("Term Propagation", test: testTermPropagation))
        await results.append(runTest("Leader Hint", test: testLeaderHint))
        await results.append(runTest("Network Partition", test: testNetworkPartition))

        let totalDuration = Date().timeIntervalSince(startTime)
        return RaftTestSuiteResult(results: results, totalDuration: totalDuration)
    }

    /// Heals the partition before running a test
    private func beforeTest() async throws {
        // Heal partition in case it exists
        try await partitionController.healPartition()
    }

    /// Heals the partition after running a test
    private func afterTest() async throws {
        try await partitionController.healPartition()
    }

    /// Runs an individual test with error handling and timing
    /// - Parameters:
    ///   - name: The name of the test.
    ///   - test: The test to run.
    /// - Returns: The result of the test.
    private func runTest(_ name: String, test: () async throws -> Void) async -> RaftTestResult {
        let startTime = Date()

        do {
            try await beforeTest()
            try await test()
            try await afterTest()
            let duration = Date().timeIntervalSince(startTime)
            logger.info("✅ Test '\(name)' passed in \(String(format: "%.2f", duration))s")
            return RaftTestResult(testName: name, success: true, duration: duration)
        } catch {
            try? await afterTest()
            let duration = Date().timeIntervalSince(startTime)
            logger.error("❌ Test '\(name)' failed in \(String(format: "%.2f", duration))s: \(error)")
            return RaftTestResult(testName: name, success: false, error: error, duration: duration)
        }
    }

    // MARK: - Individual Tests

    /// Tests that exactly one leader is elected
    public func testLeaderElection() async throws {
        logger.info("Running leader election test...")

        var leaders: [Peer] = []

        for peer in client.peers {
            do {
                let response = try await client.getServerState(of: peer)
                if response.state == .leader {
                    leaders.append(peer)
                }
            } catch {
                logger.warning("Failed to get state from peer \(peer): \(error)")
            }
        }

        guard !leaders.isEmpty else {
            throw RaftTestError.noLeaderFound
        }

        guard leaders.count == 1 else {
            throw RaftTestError.multipleLeadersFound(count: leaders.count)
        }
    }

    /// Tests log replication across all nodes
    public func testLogReplication() async throws {
        logger.info("Running log replication test...")

        let leader = try await client.findLeader()
        let testKey = "test-replication-\(UUID().uuidString)"
        let testValue = "test-value-\(Date().timeIntervalSince1970)"

        // Append entry to leader
        let putRequest = PutRequest(key: testKey, value: testValue)
        let putResponse = try await client.put(request: putRequest, to: leader)

        guard putResponse.success else {
            throw RaftTestError.operationFailed(operation: "put", reason: "Leader rejected put request")
        }

        // Wait for replication
        try await Task.sleep(for: config.replicationTimeout)

        // Verify replication on all nodes
        for peer in client.peers {
            let getRequest = GetRequest(key: testKey)
            let getResponse = try await client.getDebug(request: getRequest, from: peer)

            guard getResponse.value == testValue else {
                throw RaftTestError.entryNotReplicated(
                    key: testKey,
                    peer: peer,
                    expected: testValue,
                    actual: getResponse.value
                )
            }
        }
    }

    /// Tests leader failover scenario
    public func testLeaderFailover() async throws {
        logger.info("Running leader failover test...")

        // For good measure do it three times, since I had an error where
        // the heartbeat task cancelled and the leader was not elected, but
        // this only lead to a repetition of this test failing, the first
        // run passed.
        for _ in 0 ..< 3 {
            let originalLeader = try await client.findLeader()

            let leaderGroup = [originalLeader]
            let followerGroup = client.peers.filter { $0.id != originalLeader.id }

            try await partitionController.createPartition(group1: leaderGroup, group2: followerGroup)

            // Wait for new leader to be elected
            try await Task.sleep(for: config.electionTimeout)

            let newLeader = try await client.findLeader(excludingPeer: originalLeader)

            guard newLeader != originalLeader else {
                throw RaftTestError.operationFailed(operation: "findLeader", reason: "No new leader elected")
            }

            try await partitionController.healPartition()
            try await Task.sleep(for: config.electionTimeout)
        }
    }

    /// Tests term propagation across nodes
    public func testTermPropagation() async throws {
        logger.info("Running term propagation test...")

        var terms: [(Peer, Int)] = []

        // Collect terms from all nodes
        for peer in client.peers {
            do {
                let response = try await client.getServerTerm(of: peer)
                terms.append((peer, response.term))
            } catch {
                logger.warning("Failed to get term from peer \(peer): \(error)")
            }
        }

        guard !terms.isEmpty else {
            throw RaftTestError.operationFailed(operation: "getServerTerm", reason: "No nodes responded")
        }

        // All functioning nodes should have the same term
        let firstTerm = terms.first!.1
        let inconsistentTerms = terms.filter { $0.1 != firstTerm }

        guard inconsistentTerms.isEmpty else {
            throw RaftTestError.inconsistentTerms(terms: terms)
        }
    }

    /// Tests network partition handling
    public func testNetworkPartition() async throws {
        logger.info("Running network partition test...")

        let leader = try await client.findLeader()
        let majority = client.peers.count / 2 + 1

        // Create partition with leader in majority
        var majorityGroup = [leader]
        var minorityGroup: [Peer] = []

        for peer in client.peers where peer.id != leader.id {
            if majorityGroup.count < majority {
                majorityGroup.append(peer)
            } else {
                minorityGroup.append(peer)
            }
        }

        // Create partition
        try await partitionController.createPartition(group1: majorityGroup, group2: minorityGroup)

        // Test operations on majority side
        let testKey = "partition-test-\(UUID().uuidString)"
        let testValue = "majority-value"

        let putRequest = PutRequest(key: testKey, value: testValue)
        let putResponse = try await client.put(request: putRequest, to: leader)

        guard putResponse.success else {
            throw RaftTestError.operationFailed(operation: "put during partition", reason: "Majority partition should accept writes")
        }

        // Wait for replication within majority
        try await Task.sleep(for: config.replicationTimeout)

        // Verify replication in majority partition
        for peer in majorityGroup {
            let getRequest = GetRequest(key: testKey)
            let getResponse = try await client.getDebug(request: getRequest, from: peer)

            guard getResponse.value == testValue else {
                throw RaftTestError.entryNotReplicated(
                    key: testKey,
                    peer: peer,
                    expected: testValue,
                    actual: getResponse.value
                )
            }
        }

        // Verify not replicated in minority partition
        for peer in minorityGroup {
            let getRequest = GetRequest(key: testKey)
            let getResponse = try await client.getDebug(request: getRequest, from: peer)

            guard getResponse.value != testValue else {
                throw RaftTestError.entryReplicated(
                    key: testKey,
                    peer: peer,
                    expected: testValue,
                    actual: getResponse.value
                )
            }
        }

        // Heal partition
        try await partitionController.healPartition()

        // Wait for recovery
        try await Task.sleep(for: config.electionTimeout)

        // Verify all nodes eventually get the entry
        for peer in client.peers {
            let getRequest = GetRequest(key: testKey)
            let getResponse = try await client.getDebug(request: getRequest, from: peer)

            guard getResponse.value == testValue else {
                throw RaftTestError.entryNotReplicated(
                    key: testKey,
                    peer: peer,
                    expected: testValue,
                    actual: getResponse.value
                )
            }
        }
    }

    /// Tests leader hint functionality
    public func testLeaderHint() async throws {
        logger.info("Running leader hint test...")

        let leader = try await client.findLeader()

        // Test with non-leader nodes
        for peer in client.peers where peer != leader {
            let putRequest = PutRequest(key: "hint-test", value: "test-value")
            let putResponse = try await client.put(request: putRequest, to: peer)

            guard !putResponse.success else {
                throw RaftTestError.operationFailed(operation: "put to non-leader", reason: "Non-leader should reject writes")
            }

            guard let hintedLeader = putResponse.leaderHint, hintedLeader.id == leader.id else {
                throw RaftTestError.operationFailed(operation: "leader hint", reason: "Incorrect or missing leader hint")
            }

            let getRequest = GetRequest(key: "hint-test")
            let getResponse = try await client.get(request: getRequest, from: peer)

            guard getResponse.value == nil else {
                throw RaftTestError.operationFailed(operation: "get from non-leader", reason: "Non-leader should reject reads")
            }

            guard let getHintedLeader = getResponse.leaderHint, getHintedLeader.id == leader.id else {
                throw RaftTestError.operationFailed(operation: "get leader hint", reason: "Incorrect or missing leader hint")
            }
        }
    }
}

// MARK: - Extensions

public extension RaftTestClient {
    /// Prints a formatted test report
    func printTestReport(_ result: RaftTestSuiteResult) {
        logger.info("""

        ═══════════════════════════════════════
                  RAFT TEST REPORT
        ═══════════════════════════════════════

        Total Tests: \(result.results.count)
        Passed: \(result.passedTests.count)
        Failed: \(result.failedTests.count)
        Success Rate: \(String(format: "%.1f", result.successRate * 100))%
        Total Duration: \(String(format: "%.2f", result.totalDuration))s

        Test Results:
        """)

        for test in result.results {
            let status = test.success ? "✅ PASS" : "❌ FAIL"
            let duration = String(format: "%.2f", test.duration)
            logger.info("  \(status) \(test.testName) (\(duration)s)")

            if let error = test.error {
                logger.info("    Error: \(error.localizedDescription)")
            }
        }

        if !result.failedTests.isEmpty {
            logger.info("\nFailed Test Details:")
            for failed in result.failedTests {
                logger.info("• \(failed.testName): \(failed.error?.localizedDescription ?? "Unknown error")")
            }
        }

        logger.info("═══════════════════════════════════════")
    }
}
