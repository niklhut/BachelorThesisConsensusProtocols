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

    /// Randomness to ensure unique values across runs
    let randomness = UUID()

    /// The leader of the cluster.
    var leader: Peer?

    /// The test suite name.
    var testSuite: String?

    /// The number of CPU cores available to each node.
    var cpuCores: Double?

    /// The amount of memory (in GB) available to each node.
    var memory: Double?

    /// Initializes a new instance of the StressTestClient class.
    /// - Parameters:
    ///   - client: The Raft client to use for communication with the server.
    public init(client: RaftClient<Transport>, testSuite: String?, cpuCores: Double?, memory: Double?) {
        self.client = client
        self.testSuite = testSuite
        self.cpuCores = cpuCores
        self.memory = memory
    }

    /// Runs the stress test client.
    /// - Parameters:
    ///   - operations: The number of operations to perform.
    ///   - concurrency: The number of concurrent operations to perform.
    public func run(operations: Int, concurrency: Int, timeout: TimeInterval, durationSeconds: Int?, skipSanityCheck: Bool) async throws {
        let startTime = Date()
        if let durationSeconds {
            logger.info("Starting stress test for \(durationSeconds)s, concurrency=\(concurrency)")
        } else {
            logger.info("Starting stress test with \(operations) operations, concurrency=\(concurrency)")
        }

        leader = try await client.findLeader()

        var nextOperationIndex = concurrency
        let targetDuration: TimeInterval? = durationSeconds.flatMap { TimeInterval($0) }

        // Aggregation task that runs the load and collects metrics
        let aggregationTask = Task { () async -> RaftStressTestResult in
            await withTaskGroup(of: (success: Bool, latency: Double).self) { group in
                // Initialize with 'concurrency' number of tasks
                let initialTasks = (durationSeconds != nil) ? concurrency : min(concurrency, operations)
                for i in 0 ..< initialTasks {
                    group.addTask { [weak self] in
                        guard let self else { return (false, 0) }
                        if Task.isCancelled { return (false, 0) }
                        return await putEntry(makePutRequest(index: i))
                    }
                }

                // Process results and maintain concurrency
                var completed = 0
                var successful = 0
                var failed = 0
                var totalLatency = 0.0

                for await result in group {
                    if Task.isCancelled { break }
                    completed += 1

                    if result.success {
                        successful += 1
                        totalLatency += result.latency
                    } else {
                        failed += 1
                    }

                    // Add a new task if there are operations remaining
                    let elapsed = Date().timeIntervalSince(startTime)
                    let durationReached = targetDuration.map { elapsed >= $0 } ?? false
                    let timeoutReached = (durationSeconds == nil) ? (elapsed >= timeout) : false
                    if Task.isCancelled || durationReached || timeoutReached {
                        // Do not enqueue new work once duration/timeout reached
                        logger.info("Duration/timeout reached or cancelled, not adding new tasks")
                    } else if (durationSeconds != nil) || (nextOperationIndex < operations) {
                        let nextIndex = nextOperationIndex
                        nextOperationIndex += 1

                        group.addTask { [weak self] in
                            guard let self else { return (false, 0) }
                            if Task.isCancelled { return (false, 0) }
                            return await putEntry(makePutRequest(index: nextIndex))
                        }
                    }

                    // Stop consuming when: duration reached (strict), timeout/cancel,
                    // or in operations mode: when all operations completed.
                    let operationsCompleted = (durationSeconds == nil) && (completed == operations)
                    if durationReached || timeoutReached || operationsCompleted || Task.isCancelled {
                        group.cancelAll()
                        break
                    }
                }

                // Cancel any remaining tasks (shouldn't be necessary, but just in case)
                group.cancelAll()

                let testDuration = Date().timeIntervalSince(startTime)
                let averageLatency = successful > 0 ? totalLatency / Double(successful) : 0
                let throughput = testDuration > 0 ? Double(completed) / testDuration : 0

                let result = RaftStressTestResult(
                    start: startTime,
                    end: Date(),
                    messagesSent: completed,
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
        }
        let result = await aggregationTask.value

        logger.info("Stress test completed")

        #if !DEBUG
            // If stop was requested and allowed to send partial, still send analytics
            if await StressTestRuntime.shared.allowPartialOnStop {
                try await sendStressTestData(result)
            } else {
                try await sendStressTestData(result)
            }
        #endif

        if !skipSanityCheck {
            try await sanityCheck(concurrency: concurrency)
        }
    }

    // MARK: - Helpers

    /// Creates a PutRequest with unique key and value based on the index and randomness
    /// - Parameter index: The index to use for generating the key and value
    /// - Returns: A PutRequest with the generated key and value
    func makePutRequest(index: Int) -> PutRequest {
        PutRequest(key: "stress-key-\(index)", value: "stress-value-\(index)-\(randomness.uuidString)")
    }

    /// Execute a single operation with leader failover handling
    ///
    /// - Parameters:
    /// - value: The log entry value to append
    private func putEntry(
        _ value: PutRequest,
        maxAttempts: Int = 30,
    ) async -> (success: Bool, latency: Double) {
        let start = Date()
        var attempts = 0

        while !Task.isCancelled, attempts < maxAttempts {
            attempts += 1
            guard let currentLeader = leader else {
                // try to find leader once
                do {
                    leader = try await client.findLeader()
                } catch {
                    // small backoff then retry
                    try? await Task.sleep(for: .milliseconds(100))
                    continue
                }
                continue
            }

            do {
                let result = try await client.put(request: value, to: currentLeader)
                let latency = Date().timeIntervalSince(start) * 1000

                if let leaderHint = result.leaderHint {
                    leader = leaderHint
                    // retry with new leader (loop continues)
                    await Task.yield()
                    continue
                } else if !result.success {
                    // backoff and re-resolve leader
                    try await Task.sleep(for: .milliseconds(100))
                    leader = try await client.findLeader()
                    continue
                } else {
                    return (true, latency)
                }
            } catch {
                if Task.isCancelled { break }
                // backoff and retry
                try? await Task.sleep(for: .milliseconds(100))
                continue
            }
        }

        return (false, 0)
    }

    /// Performs a sanity check to see that all operations are actually persisted on all nodes
    private func sanityCheck(concurrency: Int) async throws {
        logger.info("Performing sanity check to see that all operations are actually persisted on all nodes")
        try await Task.sleep(for: .seconds(1))
        try await client.resetClients()

        // Generate test values
        let testValues = (0 ..< 100).map { makePutRequest(index: $0) }

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

        let baseUrl = await StressTestRuntime.shared.baseUrl ?? ProcessInfo.processInfo.environment["STRESS_TEST_BASE_URL"] ?? "http://localhost:3000"
        logger.info("Sending stress test data to \(baseUrl)")
        guard let url = URL(string: baseUrl + "/api/stress-test") else { return }

        let machineName = await StressTestRuntime.shared.machineName ?? ProcessInfo.processInfo.environment["STRESS_TEST_MACHINE_NAME"] ?? "Unknown"

        let payload = RaftStressTestPayload(
            messagesSent: result.messagesSent,
            successfulMessages: result.successfulMessages,
            averageLatency: result.averageLatency,
            averageThroughput: result.averageThroughput,
            totalDuration: result.totalDuration,
            concurrency: result.concurrency,
            compactionThreshold: implementationVersion.compactionThreshold,
            machine: RaftStressTestPayload.RaftMachineInfo(
                name: machineName,
                cpu: cpuCores,
                memory: memory,
            ),
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
        let apiKey = await StressTestRuntime.shared.apiKey ?? ProcessInfo.processInfo.environment["STRESS_TEST_API_KEY"] ?? ""
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
