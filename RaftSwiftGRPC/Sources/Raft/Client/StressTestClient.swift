import ConsoleKitTerminal
import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import Logging

actor StressTestClient {
    let client: RaftClient
    let terminal = Terminal()
    let logger = Logger(label: "raft.StressTestClient")
    var leader: Raft_Peer?

    init(client: RaftClient) {
        self.client = client
    }

    func run(operations: Int = 10000, concurrency: Int = 10) async throws {
        let startTime = Date()

        let progressBar = terminal.progressBar(title: "Stress Test")

        progressBar.start()

        leader = try await client.findLeader()

        // Pre-generate all test values before starting the test
        let baseKeys = (0 ..< 100).map { "stress-key-\($0)" }
        let testValues = (0 ..< operations).map { i in
            Raft_PutRequest(key: baseKeys[i % baseKeys.count], value: "stress-value-\(i)-\(UUID().uuidString)")
        }

        var nextOperationIndex = concurrency

        // Use task group to maintain constant concurrency
        return await withTaskGroup(of: (success: Bool, latency: Double).self) { group in
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
    }

    // MARK: - Helpers

    /// Execute a single operation with leader failover handling
    ///
    /// - Parameters:
    ///   - value: The log entry value to append
    ///   - startTime: The start time of the operation
    /// - Returns: A tuple containing the success status and latency
    func putEntry(
        _ value: Raft_PutRequest,
        startTime: Date = Date()
    ) async -> (success: Bool, latency: Double) {
        guard let currentLeader = leader else {
            return (false, 0)
        }

        do {
            let result = try await client.put(request: value, to: currentLeader)

            if result.hasLeaderHint {
                leader = result.leaderHint
                return await putEntry(value, startTime: startTime)
            }

            let latency = Date().timeIntervalSince(startTime) * 1000
            return (result.success, latency)
        } catch {
            return await putEntry(value, startTime: startTime)
        }
    }
}
