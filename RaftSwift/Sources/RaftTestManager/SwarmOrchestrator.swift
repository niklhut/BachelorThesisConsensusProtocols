import Foundation
import Logging
import RaftCore
import RaftDistributedActorsTransport
import RaftGRPCTransport
import RaftTest

/// Swift rewrite of test_swarm_distributed runner tailored for Swift package.
public final class SwarmOrchestrator: @unchecked Sendable {
    private let servicePrefix = "nhuthmann_"
    private var servers: [(String, String)] = [
        ("zs04.lab.dm.informatik.tu-darmstadt.de", "10.10.2.184"),
        ("zs05.lab.dm.informatik.tu-darmstadt.de", "10.10.2.185"),
        ("zs07.lab.dm.informatik.tu-darmstadt.de", "10.10.2.187"),
        ("zs02.lab.dm.informatik.tu-darmstadt.de", "10.10.2.182"),
        ("zs08.lab.dm.informatik.tu-darmstadt.de", "10.10.2.188"),
        ("zs01.lab.dm.informatik.tu-darmstadt.de", "10.10.2.181"),
    ]

    private var logger = Logger(label: "SwarmOrchestrator")

    private var isCleaningUp = false
    private var lastPort = 50000

    private let manager: DockerSwarmServiceManager
    private var collectMetrics: Bool
    private var timeout: TimeInterval
    private var retries: Int
    private var repetitions: Int
    private var persistence: TestPersistence
    private var testSuiteName: String
    private var resumeFromTestNumber: Int?
    private var testDurationSeconds: Int?

    public init(
        collectMetrics: Bool = true,
        timeout: TimeInterval = 180,
        retries: Int = 1,
        repetitions: Int = 3,
        persistence: TestPersistence = .file,
        testSuiteName: String? = nil
    ) {
        logger.logLevel = .debug
        manager = DockerSwarmServiceManager(servicePrefix: servicePrefix, logger: logger)
        self.collectMetrics = collectMetrics
        self.timeout = timeout
        self.retries = retries
        self.repetitions = repetitions
        self.persistence = persistence
        self.testSuiteName = testSuiteName ?? "Raft Swarm Test Suite \(ISO8601DateFormatter().string(from: Date()))"
    }

    // Allow runtime overrides from scenario root
    public func applyGlobalOverrides(images: [String]?, timeout: Int?, retries: Int?, repetitions: Int?, persistence: TestPersistence?, collectMetrics: Bool?, testSuiteName: String? = nil, resumeFromTestNumber: Int? = nil, testDurationSeconds: Int? = nil) {
        // images handled by caller
        if let t = timeout { self.timeout = TimeInterval(t) }
        if let r = retries { self.retries = r }
        if let rep = repetitions { self.repetitions = rep }
        if let p = persistence { self.persistence = p }
        if let cm = collectMetrics { self.collectMetrics = cm }
        if let ts = testSuiteName { self.testSuiteName = ts }
        if let rs = resumeFromTestNumber { self.resumeFromTestNumber = rs }
        if let td = testDurationSeconds { self.testDurationSeconds = td }
        logger.info("Applied global overrides: timeout=\(self.timeout)s, retries=\(self.retries), repetitions=\(self.repetitions), persistence=\(self.persistence), collectMetrics=\(self.collectMetrics), testSuiteName='\(self.testSuiteName)', resumeFromTestNumber=\(String(describing: self.resumeFromTestNumber)), durationSeconds=\(String(describing: self.testDurationSeconds)))")
    }

    public enum Outcome: Sendable { case success, failure, timeout }

    // MARK: - Public API

    public func run(using scenariosURL: URL, images: [String]? = nil) async throws {
        let data = try Data(contentsOf: scenariosURL)
        let decoder = JSONDecoder()
        let scenarios: [ScenarioConfig]
        var analytics: AnalyticsSettings?
        var imagesToUse: [String] = []
        if let root = try? decoder.decode(ScenarioRoot.self, from: data) {
            scenarios = root.scenarios
            analytics = root.analytics
            if let sv = root.servers, !sv.isEmpty {
                servers = sv.map { ($0.hostname, $0.ip) }
            }
            if let rootImages = root.images, !rootImages.isEmpty {
                imagesToUse = rootImages
            } else if let imgs = images, !imgs.isEmpty {
                imagesToUse = imgs
            }
        } else {
            scenarios = try decoder.decode([ScenarioConfig].self, from: data)
            imagesToUse = images ?? []
        }
        if imagesToUse.isEmpty {
            throw NSError(domain: "SwarmOrchestrator", code: 1, userInfo: [NSLocalizedDescriptionKey: "No Docker images provided in scenario JSON root or CLI --images option"])
        }
        let combinations = generateCombinations(from: scenarios, images: imagesToUse)
        let totalNumberOfTests = combinations.count

        // Ensure clean state before starting
        cleanup()

        for (index, combo) in combinations.enumerated() {
            let testNumber = index + 1

            if let resumeFrom = resumeFromTestNumber, testNumber < resumeFrom {
                logger.info("Skipping test \(testNumber) / \(totalNumberOfTests) to resume from test number \(resumeFrom)")
                continue
            }

            try await runSingleTest(testNumber: testNumber, totalNumberOfTests: totalNumberOfTests, params: combo, analytics: analytics)
        }

        cleanup()
    }

    public func cleanup() {
        if isCleaningUp { return }
        isCleaningUp = true
        manager.removeAllServices()
        isCleaningUp = false
    }

    // MARK: - Combination generation

    private func generateCombinations(from scenarios: [ScenarioConfig], images: [String]) -> [TestCombination] {
        func listOrNil<T>(_ arr: [T]?, fallback: [T]?) -> [T]? { arr ?? fallback }
        var combos: [TestCombination] = []

        for sc in scenarios {
            let fixed = sc.fixed
            let vary = sc.vary

            let compaction = listOrNil(vary?.compactionThresholds, fallback: fixed?.compactionThresholds) ?? [1000]
            let peers = listOrNil(vary?.peerCounts, fallback: fixed?.peerCounts) ?? [3]
            var ops = listOrNil(vary?.operationCounts, fallback: fixed?.operationCounts) ?? [10000]
            if testDurationSeconds != nil, let first = ops.first {
                ops = [first]
            }
            let conc = listOrNil(vary?.concurrencyLevels, fallback: fixed?.concurrencyLevels) ?? [2]

            // instance sizes precedence
            let instancePairs: [(String?, String?)] = {
                if let sizes = (vary?.instanceSizes ?? fixed?.instanceSizes), !sizes.isEmpty {
                    return sizes.map { ($0.cpuLimits, $0.memoryLimits) }
                }
                let cpu = listOrNil(vary?.cpuLimits, fallback: fixed?.cpuLimits)
                let mem = listOrNil(vary?.memoryLimits, fallback: fixed?.memoryLimits)
                if let cpu, let mem {
                    return cpu.flatMap { c in mem.map { m in (Optional(c), Optional(m)) } }
                } else if let cpu {
                    return cpu.map { (Optional($0), nil) }
                } else if let mem {
                    return mem.map { (nil, Optional($0)) }
                } else {
                    return [(nil, nil)]
                }
            }()

            let persList: [TestPersistence] = {
                if let v = vary?.persistences, !v.isEmpty { return v }
                if let p = vary?.persistence { return [p] }
                if let v = fixed?.persistences, !v.isEmpty { return v }
                if let p = fixed?.persistence { return [p] }
                return [persistence]
            }()

            for image in images {
                logger.debug("Generating combinations for scenario '\(sc.name ?? "unnamed")' with image '\(image)'")
                for th in compaction {
                    logger.debug(" - Compaction: \(th)")
                    for p in peers where p <= servers.count {
                        logger.debug("   - Peers: \(p)")
                        for o in ops {
                            logger.debug("     - Operations: \(o)")
                            for c in conc {
                                logger.debug("       - Concurrency: \(c)")
                                for (cpu, mem) in instancePairs {
                                    logger.debug("         - Instance: CPU: \(cpu ?? "N/A"), Memory: \(mem ?? "N/A")")
                                    if image.lowercased().contains("raftswift") {
                                        combos.append(TestCombination(image: image, compactionThreshold: th, peers: p, operations: o, concurrency: c, cpuLimit: cpu, memoryLimit: mem, persistence: persList.first ?? persistence, scenarioName: sc.name, useDistributedActorSystem: false, useManualLocks: false, testDurationSeconds: self.testDurationSeconds))
                                        combos.append(TestCombination(image: image, compactionThreshold: th, peers: p, operations: o, concurrency: c, cpuLimit: cpu, memoryLimit: mem, persistence: persList.first ?? persistence, scenarioName: sc.name, useDistributedActorSystem: true, useManualLocks: false, testDurationSeconds: self.testDurationSeconds))
                                    } else {
                                        combos.append(TestCombination(image: image, compactionThreshold: th, peers: p, operations: o, concurrency: c, cpuLimit: cpu, memoryLimit: mem, persistence: persList.first ?? persistence, scenarioName: sc.name, useDistributedActorSystem: false, useManualLocks: false, testDurationSeconds: self.testDurationSeconds))
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        logger.info("Generated \(combos.count) test combinations from \(scenarios.count) scenarios and \(images.count) images")

        return combos
    }

    // MARK: - Single test execution

    private func runSingleTest(testNumber: Int, totalNumberOfTests: Int, params: TestCombination, analytics: AnalyticsSettings?) async throws {
        logger.info("Starting test \(testNumber) / \(totalNumberOfTests) with parameters: \(params.description)")
        var repetitionResults: [RepetitionResult] = []

        // Select servers for peers
        let nodeServers = Array(servers.prefix(params.peers).map(\.0))
        let port = lastPort
        lastPort += 1
        if lastPort > 50100 { lastPort = 50000 }

        // Start peer containers once before repetitions
        try await startPeers(params: params, nodeServers: nodeServers, port: port)

        for r in 0 ..< repetitions {
            logger.info(" Test \(testNumber) / \(totalNumberOfTests) - Repetition \(r + 1) / \(repetitions)")
            var status = RepetitionResult.Status.timeout
            var attempt = 0

            while attempt < retries {
                logger.info(" Test \(testNumber) / \(totalNumberOfTests) - Repetition \(r + 1) / \(repetitions) - Attempt \(attempt + 1) / \(retries)")
                attempt += 1
                if r == 0, attempt > 1 {
                    logger.info("  Full restart of peers for new attempt")
                    cleanup()
                    try await startPeers(params: params, nodeServers: nodeServers, port: Int.random(in: 50000 ... 50100))
                }

                let isLastAttempt = attempt == retries
                status = await runClientOnce(params: params, nodeServers: nodeServers, port: port, analytics: analytics, allowPartialOnTimeout: isLastAttempt)
                if status == .success { break }
            }

            repetitionResults.append(RepetitionResult(repetition: r + 1, status: status))
        }

        cleanup()
    }

    private func startPeers(params: TestCombination, nodeServers: [String], port: Int) async throws {
        // Give some grace time if previous services were removed
        try await Task.sleep(for: .seconds(1))
        var peerArgs: [String] = []
        for (idx, host) in nodeServers.enumerated() {
            let nodeID = idx + 1
            let svcName = "\(servicePrefix)raft_node_\(nodeID)"

            var flags: [String] = []
            if collectMetrics { flags.append("--collect-metrics") }
            if params.useDistributedActorSystem { flags.append("--use-distributed-actor-system") }
            if params.useManualLocks { flags.append("--use-manual-lock") }
            flags.append("--persistence \(params.persistence.rawValue)")

            // Resource limits: pass through as-is to service create using docker-specific flags
            var resourceFlags = ""
            if let cpu = params.cpuLimit, !cpu.isEmpty { resourceFlags += " --limit-cpu \(cpu)" }
            if let mem = params.memoryLimit, !mem.isEmpty { resourceFlags += " --limit-memory \(mem)" }

            // Build peers config excluding self
            let peersConfig = nodeServers.enumerated().compactMap { i, peerHost -> String? in
                if peerHost == host { return nil }
                return "\(i + 1):\(servers.first { $0.0 == peerHost }!.1):\(port)"
            }.joined(separator: ",")

            let args = [
                "--name \(svcName)",
                "--network host --restart-condition none",
                "--constraint 'node.hostname==\(host)'",
                resourceFlags,
                "\(params.image) peer --id \(nodeID) --port \(port) --address \(servers.first { $0.0 == host }!.1) --peers '\(peersConfig)' --compaction-threshold \(params.compactionThreshold) \(flags.joined(separator: " "))",
            ].joined(separator: " ")
            peerArgs.append(args)
        }

        try await withThrowingTaskGroup { group in
            for args in peerArgs {
                group.addTask { [weak self] in
                    guard let self else { throw CancellationError() }
                    try manager.createService(args, background: false)
                }
            }
            try await group.waitForAll()
        }

        // Give peers time to come up
        try await Task.sleep(for: .seconds(1))
    }

    private func runClientOnce(params: TestCombination, nodeServers: [String], port: Int, analytics: AnalyticsSettings?, allowPartialOnTimeout: Bool) async -> RepetitionResult.Status {
        // Build peers list including all nodes
        let peers = nodeServers.enumerated().map { i, host in
            Peer(id: i + 1, address: servers.first { $0.0 == host }!.1, port: port)
        }

        // Instantiate the client transport
        let client: any RaftTestApplication = if params.useDistributedActorSystem {
            RaftDistributedActorClient(peers: peers)
        } else {
            RaftGRPCClient(peers: peers)
        }

        // Calculate resource hints
        let cpuCores: Double? = params.cpuLimit.flatMap(Double.init)
        let memoryGB: Double? = params.memoryLimit.flatMap(Self.normalizeMemoryToGB)

        // Configure runtime analytics for this run
        await StressTestRuntime.shared.configureAnalytics(baseUrl: analytics?.baseUrl, apiKey: analytics?.apiKey, machineName: analytics?.machineName)
        await StressTestRuntime.shared.setAllowPartialOnStop(allowPartialOnTimeout)

        let suite = testSuiteName
        do {
            try await client.runStressTest(
                operations: params.operations,
                concurrency: params.concurrency,
                testSuiteName: suite,
                timeout: timeout,
                durationSeconds: params.testDurationSeconds,
                cpuCores: cpuCores,
                memory: memoryGB,
                skipSanityCheck: true,
            )
            return .success
        } catch {
            return .failed
        }
    }

    private static func normalizeMemoryToGB(_ value: String) -> Double? {
        let s = value.trimmingCharacters(in: .whitespaces).lowercased()
        if let v = Double(s) { return v } // assume GB
        if s.hasSuffix("gb") || s.hasSuffix("g") {
            let num = s.replacingOccurrences(of: "gb", with: "").replacingOccurrences(of: "g", with: "")
            return Double(num)
        }
        if s.hasSuffix("mb") || s.hasSuffix("m") {
            let num = s.replacingOccurrences(of: "mb", with: "").replacingOccurrences(of: "m", with: "")
            if let mb = Double(num) { return mb / 1024.0 }
        }
        if s.hasSuffix("kb") || s.hasSuffix("k") {
            let num = s.replacingOccurrences(of: "kb", with: "").replacingOccurrences(of: "k", with: "")
            if let kb = Double(num) { return kb / (1024.0 * 1024.0) }
        }
        if let bytes = Double(s) { return bytes / pow(1024.0, 3) }
        return nil
    }
}
