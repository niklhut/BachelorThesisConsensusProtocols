import Distributed
import DistributedCluster
import Foundation
import Logging

// Types needed for client operations
extension DistributedReception.Key {
    static var raftClient: DistributedReception.Key<RaftClient> {
        "raftClient"
    }
}

distributed actor RaftClient: LifecycleWatch, PeerDiscovery {
    // MARK: - Properties

    var peers: Set<RaftNode> = []
    private var leader: RaftNode?
    var listingTask: Task<Void, Never>?

    private var majority: Int {
        peers.count / 2 + 1
    }

    init(actorSystem: ActorSystem) {
        self.actorSystem = actorSystem
    }

    // MARK: - Core Client Methods

    distributed func start() async {
        actorSystem.log.info("Starting Raft client")
        findPeers()
    }

    distributed func stop() {
        listingTask?.cancel()
        listingTask = nil
        actorSystem.cluster.leave()
    }

    /// Find the current leader in the cluster
    private func findLeader() async {
        if peers.isEmpty {
            actorSystem.log.warning("No peers available to determine leader. Waiting before retrying.")
            try? await Task.sleep(for: .seconds(1))
            return await findLeader()
        }

        while leader == nil {
            let peer = peers.randomElement()!
            do {
                try await peer.appendClientEntries(entries: [])
                if leader?.id != peer.id {
                    actorSystem.log.info("Found new leader: \(peer.id)")
                    leader = peer
                }
                return
            } catch let RaftError.notLeader(leaderId) where leaderId != nil {
                actorSystem.log.info("Found new leader: \(leaderId!)")
                leader = peers.first(where: { $0.id == leaderId })
                return
            } catch {
                // Not the leader, continue trying other peers
                actorSystem.log.trace("Peer \(peer.id) is not the leader: \(error)")
            }
        }
    }

    // MARK: - Test Methods

    /// Run basic correctness tests for log replication
    distributed func runCorrectnessTest() async throws -> TestResult {
        actorSystem.log.info("Starting correctness test")
        let startTime = Date()

        let testValues = [
            LogEntryValue(key: "test1", value: "value1"),
            LogEntryValue(key: "test2", value: "value2"),
            LogEntryValue(key: "test3", value: "value3"),
            LogEntryValue(key: "configuration", value: "production"),
            LogEntryValue(key: "database", value: "mysql"),
            LogEntryValue(key: "cache", value: "redis"),
            LogEntryValue(key: "mode", value: "async"),
            LogEntryValue(key: "timeout", value: "30s"),
            LogEntryValue(key: "retries", value: "3"),
            LogEntryValue(key: "maxConnections", value: "100"),
        ]

        var successful = 0
        var failed = 0
        var totalLatency = 0.0
        let operations = testValues.count

        // Ensure we have the latest leader
        await findLeader()

        guard let currentLeader = leader else {
            actorSystem.log.error("No leader available for test")
            throw TestError.noLeaderAvailable
        }

        // Submit each test value and verify replication
        for i in 0 ..< operations {
            let testValue = testValues[i % testValues.count]
            let operationStart = Date()

            do {
                // Submit the operation to the leader
                try await currentLeader.appendClientEntries(entries: [testValue])

                // Wait a moment for replication
                try await Task.sleep(for: .milliseconds(500))

                // Verify the entry was replicated to at least a majority of nodes
                let verificationResults = await withTaskGroup(of: Bool.self) { group in
                    for peer in peers {
                        group.addTask {
                            do {
                                let value = try await peer.getStateValue(key: testValue.key)
                                return value == testValue.value
                            } catch {
                                return false
                            }
                        }
                    }

                    var successCount = 0
                    for await result in group where result {
                        successCount += 1
                    }

                    return successCount >= majority
                }

                let latency = Date().timeIntervalSince(operationStart) * 1000 // Convert to ms
                totalLatency += latency

                if verificationResults {
                    successful += 1
                    actorSystem.log.info("Test value \(testValue.key) successfully replicated")
                } else {
                    failed += 1
                    actorSystem.log.warning(
                        "Test value \(testValue.key) replication verification failed")
                }
            } catch {
                failed += 1
                actorSystem.log.error("Failed to append test value \(testValue.key): \(error)")

                // If we encounter an error, the leader might have changed
                await findLeader()
            }
        }

        let testDuration = Date().timeIntervalSince(startTime)
        let averageLatency = successful > 0 ? totalLatency / Double(successful) : 0
        let throughput = testDuration > 0 ? Double(successful) / testDuration : 0

        let result = TestResult(
            totalOperations: operations,
            successfulOperations: successful,
            failedOperations: failed,
            averageLatency: averageLatency,
            throughput: throughput,
            testDuration: testDuration
        )

        actorSystem.log.info(
            """
            Correctness Test Results:
            - Success Rate: \(successful)/\(operations) (\(Double(successful) / Double(operations) * 100)%)
            - Average Latency: \(averageLatency) ms
            - Throughput: \(throughput) ops/sec
            - Duration: \(testDuration) seconds
            """)

        return result
    }

    /// Run a stress test for throughput and latency measurement
    ///
    /// - Parameters:
    ///   - operations: The number of operations to perform.
    ///   - concurrency: The number of concurrent operations.
    /// - Returns: The test results.
    distributed func runStressTest(operations: Int = 1000, concurrency: Int = 10) async throws -> TestResult {
        actorSystem.log.info(
            "Starting stress test with \(operations) operations and concurrency level \(concurrency)"
        )
        let startTime = Date()

        var successful = 0
        var failed = 0
        var totalLatency = 0.0

        await findLeader()

        guard let currentLeader = leader else {
            actorSystem.log.error("No leader available for test")
            throw TestError.noLeaderAvailable
        }

        // TODO: have a fixed number of keys but a different value for each operation
        // Create a reusable set of test values
        let baseTestValues = (0 ..< 100).map { i in
            LogEntryValue(key: "stress-key-\(i)", value: "stress-value-\(i)-\(UUID().uuidString)")
        }

        // Run concurrent operations
        await withTaskGroup(of: (success: Bool, latency: Double).self) { group in
            for i in 0 ..< operations {
                group.addTask {
                    let operationStart = Date()
                    let testValue = baseTestValues[i % baseTestValues.count]

                    do {
                        // TODO: i guess a handle client request helper would be helpful. It should handle leader changes
                        try await currentLeader.appendClientEntries(entries: [testValue])
                        let latency = Date().timeIntervalSince(operationStart) * 1000 // Convert to ms
                        return (true, latency)
                    } catch {
                        return (false, 0)
                    }
                }

                // Control concurrency level
                if (i + 1) % concurrency == 0 {
                    // Wait for all current tasks to complete before adding more
                    for await result in group {
                        if result.success {
                            successful += 1
                            totalLatency += result.latency
                        } else {
                            failed += 1
                        }
                    }

                    // Check if leader has changed
                    if failed > successful {
                        await findLeader()
                        if let newLeader = leader, newLeader.id != currentLeader.id {
                            actorSystem.log.info(
                                "Leader changed during stress test to \(newLeader.id)")
                        }
                    }
                }
            }

            // Collect any remaining results
            for await result in group {
                if result.success {
                    successful += 1
                    totalLatency += result.latency
                } else {
                    failed += 1
                }
            }
        }

        let testDuration = Date().timeIntervalSince(startTime)
        let averageLatency = successful > 0 ? totalLatency / Double(successful) : 0
        let throughput = testDuration > 0 ? Double(successful) / testDuration : 0

        let result = TestResult(
            totalOperations: operations,
            successfulOperations: successful,
            failedOperations: failed,
            averageLatency: averageLatency,
            throughput: throughput,
            testDuration: testDuration,
        )

        actorSystem.log.info(
            """
            Stress Test Results:
            - Success Rate: \(successful)/\(operations) (\(Double(successful) / Double(operations) * 100)%)
            - Average Latency: \(averageLatency) ms
            - Throughput: \(throughput) ops/sec
            - Duration: \(testDuration) seconds
            - Concurrency Level: \(concurrency)
            """)

        return result
    }

    // MARK: - Lifecycle

    func terminated(actor id: DistributedCluster.ActorID) async {
        if let peerToRemove = peers.first(where: { $0.id == id }) {
            peers.remove(peerToRemove)
            actorSystem.log.warning("Peer \(id) terminated")
        }

        if leader?.id == id {
            actorSystem.log.warning("Leader node \(id) terminated")
            leader = nil
            await findLeader()
        }
    }

    deinit {
        listingTask?.cancel()
    }
}
