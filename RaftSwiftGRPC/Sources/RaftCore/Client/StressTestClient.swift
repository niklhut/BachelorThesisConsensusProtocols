import ConsoleKitTerminal
import Foundation
import Logging

/// Stress test client for Raft.
/// Allows to perform a stress test on the Raft cluster.
public actor StressTestClient {
    /// The Raft client to use for communication with the server.
    let client: RaftClient

    /// The logger to use for logging.
    let logger = Logger(label: "raft.StressTestClient")

    /// The terminal to use for output.
    let terminal = Terminal()

    /// The leader of the cluster.
    var leader: Peer?

    /// Initializes a new instance of the StressTestClient class.
    /// - Parameters:
    ///   - client: The Raft client to use for communication with the server.
    public init(client: RaftClient) {
        self.client = client
    }

    /// Runs the stress test client.
    /// - Parameters:
    ///   - operations: The number of operations to perform.
    ///   - concurrency: The number of concurrent operations to perform.
    public func run(operations: Int = 10000, concurrency: Int = 10) async throws {
        let startTime = Date()

        let progressBar = terminal.progressBar(title: "Stress Test")

        progressBar.start()

        leader = try await client.findLeader()

        // Pre-generate all test values before starting the test
        let testValues = (0 ..< operations).map { i in
            PutRequest(key: "stress-key-\(i)", value: "stress-value-\(i)-\(UUID().uuidString)")
        }

        var nextOperationIndex = concurrency

        // Use task group to maintain constant concurrency
        await withTaskGroup(of: (success: Bool, latency: Double).self) { group in
            // Initialize with 'concurrency' number of tasks
            for i in 0 ..< min(concurrency, operations) {
                group.addTask {
                    await self.putEntry(testValues[i])
                }
            }

            // Process results and maintain concurrency
            var completed = 0
            var successful = 0
            var failed = 0
            var totalLatency = 0.0

            for await result in group {
                completed += 1
                progressBar.activity.currentProgress = Double(completed) / Double(operations)

                if result.success {
                    successful += 1
                    totalLatency += result.latency
                } else {
                    failed += 1
                }

                // Add a new task if there are operations remaining
                if nextOperationIndex < operations {
                    let nextIndex = nextOperationIndex
                    nextOperationIndex += 1

                    group.addTask {
                        await self.putEntry(testValues[nextIndex])
                    }
                }

                if completed == operations {
                    break
                }
            }

            // Cancel any remaining tasks (shouldn't be necessary, but just in case)
            group.cancelAll()
            progressBar.succeed()

            let testDuration = Date().timeIntervalSince(startTime)
            let averageLatency = successful > 0 ? totalLatency / Double(operations) : 0
            let throughput = testDuration > 0 ? Double(operations) / testDuration : 0

            logger.info(
                """
                Stress Test Results:
                - Success Rate: \(successful)/\(operations) (\(Double(successful) / Double(operations) * 100)%)
                - Average Latency: \(averageLatency) ms
                - Throughput: \(throughput) ops/sec
                - Duration: \(testDuration) seconds
                - Concurrency Level: \(concurrency)
                """
            )
        }

        // Perform sanity check to see that all operations are actually persisted on all nodes
        logger.info("Performing sanity check to see that all operations are actually persisted on all nodes")

        try await withThrowingTaskGroup { group in
            for value in testValues {
                for peer in self.client.peers {
                    group.addTask {
                        let getRequest = GetRequest(key: value.key)
                        let response = try await self.client.getDebug(request: getRequest, to: peer)
                        if response.value != value.value {
                            self.logger.error("Value mismatch for key \(value.key) on peer \(peer): expected \(String(describing: value.value)), got \(String(describing: response.value))")
                        }
                    }
                }
            }

            try await group.waitForAll()
        }
    }

    // MARK: - Helpers

    /// Execute a single operation with leader failover handling
    ///
    /// - Parameters:
    /// - value: The log entry value to append
    /// - startTime: The start time of the operation
    /// - Returns: A tuple containing the success status and latency
    private func putEntry(
        _ value: PutRequest,
        startTime: Date = Date()
    ) async -> (success: Bool, latency: Double) {
        guard let currentLeader = leader else {
            return (false, 0)
        }

        do {
            let result = try await client.put(request: value, to: currentLeader)

            if let leaderHint = result.leaderHint {
                leader = leaderHint
                return await putEntry(value, startTime: startTime)
            }

            let latency = Date().timeIntervalSince(startTime) * 1000
            return (result.success, latency)
        } catch {
            return await putEntry(value, startTime: startTime)
        }
    }
}
