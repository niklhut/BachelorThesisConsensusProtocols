import ConsoleKitTerminal
import Foundation
#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Logging
import RaftCore

/// Stress test client for Raft.
/// Allows to perform a stress test on the Raft cluster.
public actor StressTestClient<Transport: RaftClientTransport> {
    /// The Raft client to use for communication with the server.
    let client: RaftClient<Transport>

    /// The logger to use for logging.
    let logger = Logger(label: "raft.StressTestClient")

    /// The terminal to use for output.
    let terminal = Terminal()

    /// The leader of the cluster.
    var leader: Peer?

    /// The test suite name.
    var testSuite: String?

    /// Initializes a new instance of the StressTestClient class.
    /// - Parameters:
    ///   - client: The Raft client to use for communication with the server.
    public init(client: RaftClient<Transport>, testSuite: String?) {
        self.client = client
        self.testSuite = testSuite
    }

    /// Runs the stress test client.
    /// - Parameters:
    ///   - operations: The number of operations to perform.
    ///   - concurrency: The number of concurrent operations to perform.
    public func run(operations: Int = 10000, concurrency: Int = 10, skipSanityCheck: Bool = false) async throws {
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
        let result = await withTaskGroup(of: (success: Bool, latency: Double).self) { group in
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

            let result = RaftStressTestResult(
                start: startTime,
                end: Date(),
                messagesSent: operations,
                successfulMessages: successful,
                averageLatency: averageLatency,
                averageThroughput: throughput,
                totalDuration: testDuration,
                concurrency: concurrency,
                numberOfPeers: client.peers.count,
            )

            logger.info(.init(stringLiteral: result.description))

            return result
        }

        #if !DEBUG
            try await sendStressTestData(result)
        #endif

        if !skipSanityCheck {
            try await sanityCheck(testValues: testValues, concurrency: concurrency)
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
        startTime: Date = Date(),
    ) async -> (success: Bool, latency: Double) {
        guard let currentLeader = leader else {
            return (false, 0)
        }

        do {
            let result = try await client.put(request: value, to: currentLeader)
            let latency = Date().timeIntervalSince(startTime) * 1000

            if let leaderHint = result.leaderHint {
                leader = leaderHint
                return await putEntry(value)
            } else if !result.success {
                // Wait for leader election then retry with new leader
                try await Task.sleep(for: .milliseconds(100))
                leader = try await client.findLeader()
                return await putEntry(value)
            } else {
                return (result.success, latency)
            }
        } catch {
            return await putEntry(value)
        }
    }

    /// Performs a sanity check to see that all operations are actually persisted on all nodes
    private func sanityCheck(testValues: [PutRequest], concurrency: Int) async throws {
        logger.info("Performing sanity check to see that all operations are actually persisted on all nodes")
        try await Task.sleep(for: .seconds(1))
        try await client.resetClients()

        let sanityTasks = testValues.flatMap { value in
            self.client.peers.map { peer in
                (value, peer)
            }
        }

        let batchSize = concurrency
        var batchStartIndex = 0

        while batchStartIndex < sanityTasks.count {
            let batch = sanityTasks[batchStartIndex ..< min(batchStartIndex + batchSize, sanityTasks.count)]

            try await withThrowingTaskGroup(of: Void.self) { group in
                for (value, peer) in batch {
                    group.addTask {
                        let getRequest = GetRequest(key: value.key)
                        let response = try await self.client.getDebug(request: getRequest, from: peer)
                        if response.value != value.value {
                            self.logger.error("Value mismatch for key \(value.key) on peer \(peer): expected \(String(describing: value.value)), got \(String(describing: response.value))")
                        }
                    }
                }
                try await group.waitForAll()
            }

            batchStartIndex += batchSize
        }
    }

    /// Sends the stress test data to the server
    /// - Parameter result: The result of the stress test
    private func sendStressTestData(_ result: RaftStressTestResult) async throws {
        // First get client implementation versions
        let clientDiagnostics = try await withThrowingTaskGroup(of: DiagnosticsResponse.self) { group in
            for peer in client.peers {
                group.addTask {
                    let diagnosticsRequest = DiagnosticsRequest(start: result.start, end: result.end)
                    return try await self.client.getDiagnostics(request: diagnosticsRequest, of: peer)
                }
            }

            var diagnostics = [DiagnosticsResponse]()
            for try await version in group {
                diagnostics.append(version)
            }
            return diagnostics
        }

        guard let implementationVersion = clientDiagnostics.first else { return }
        if !clientDiagnostics.allSatisfy({
            $0.implementation == implementationVersion.implementation &&
                $0.version == implementationVersion.version &&
                $0.compactionThreshold == implementationVersion.compactionThreshold
        }) {
            logger.error("Implementation versions do not match across all nodes")
        }

        let nodes: [RaftStressTestPayload.RaftStressTestMetrics.Node] = clientDiagnostics.compactMap { diagnostics in
            guard let metrics = diagnostics.metrics else { return nil }
            let samples = metrics.map { metric in
                RaftStressTestPayload.RaftStressTestMetrics.Node.Sample(
                    measuredAt: metric.timestamp,
                    cpuUsage: metric.cpu,
                    memoryUsage: metric.memoryMB,
                )
            }
            return RaftStressTestPayload.RaftStressTestMetrics.Node(name: "Node \(diagnostics.id)", samples: samples)
        }
        let metrics = RaftStressTestPayload.RaftStressTestMetrics(nodes: nodes)

        logger.info("Metrics: \(metrics)")

        let baseUrl = ProcessInfo.processInfo.environment["STRESS_TEST_BASE_URL"] ?? "http://localhost:3000"
        logger.info("Sending stress test data to \(baseUrl)")
        guard let url = URL(string: baseUrl + "/api/stress-test") else { return }

        let machineName = ProcessInfo.processInfo.environment["STRESS_TEST_MACHINE_NAME"] ?? "Unknown"

        let payload = RaftStressTestPayload(
            messagesSent: result.messagesSent,
            successfulMessages: result.successfulMessages,
            averageLatency: result.averageLatency,
            averageThroughput: result.averageThroughput,
            totalDuration: result.totalDuration,
            concurrency: result.concurrency,
            compactionThreshold: implementationVersion.compactionThreshold,
            machine: machineName,
            numberOfPeers: result.numberOfPeers,
            peerVersion: RaftStressTestPayload.RaftImplementationVersion(
                implementation: implementationVersion.implementation,
                version: implementationVersion.version,
            ),
            testSuite: testSuite,
            metrics: metrics,
        )

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.addValue("application/json", forHTTPHeaderField: "Content-Type")
        let apiKey = ProcessInfo.processInfo.environment["STRESS_TEST_API_KEY"] ?? ""
        request.addValue(apiKey, forHTTPHeaderField: "x-api-key")

        do {
            let encoder = JSONEncoder()
            encoder.dateEncodingStrategy = .iso8601
            let jsonData = try encoder.encode(payload)
            request.httpBody = jsonData
        } catch {
            print("Failed to encode payload:", error)
            return
        }

        do {
            let (data, response) = try await URLSession.shared.data(for: request)

            guard let httpResponse = response as? HTTPURLResponse else {
                print("Invalid response")
                return
            }

            print("Response code:", httpResponse.statusCode)

            if let responseBody = String(data: data, encoding: .utf8) {
                print("Response body:", responseBody)
            }
        } catch {
            print("Request error:", error)
        }
    }
}
