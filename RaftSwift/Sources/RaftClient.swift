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

enum TestError: Error {
    case noLeaderAvailable
    case leaderCrashed
    case leaderChanged
}

enum TestType {
    case correctness
}

struct TestResult: Codable, Equatable {
    var totalOperations: Int
    var successfulOperations: Int
    var failedOperations: Int
    var averageLatency: Double // in milliseconds
    var throughput: Double // operations per second
    var testDuration: Double // in seconds
    var description: String
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
        guard !peers.isEmpty else {
            actorSystem.log.warning("No peers available to determine leader")
            return
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
            testDuration: testDuration,
            description: "Basic correctness test completed"
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
