import ArgumentParser
import Dispatch
import Foundation
import Logging
import RaftTestManager

extension TestPersistence: ExpressibleByArgument {}

final class TestManager: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "test-manager",
        abstract: "Run distributed swarm tests using scenarios.json and local client",
        discussion: "Starts peer containers in Docker Swarm and runs the Swift client directly.",
    )

    @Option(help: "Path to scenarios JSON file")
    var scenarioConfig: String

    public func run() async throws {
        LoggingSystem.bootstrap { label in
            var stdoutHandler = StreamLogHandler.standardOutput(label: label)
            stdoutHandler.logLevel = .debug // show all in console

            let now = Date()
            let dateFormatter = DateFormatter()
            dateFormatter.dateFormat = "yyyy-MM-dd-HH-mm-ss"
            let fileURL = URL(fileURLWithPath: "test-output/swarm-\(dateFormatter.string(from: now)).log")
            let fileHandler = try! FileLogHandler(label: label, fileURL: fileURL)

            return MultiplexLogHandler([stdoutHandler, fileHandler])
        }

        // Create orchestrator with defaults; overrides will be applied from JSON root when present.
        let orchestrator = SwarmOrchestrator()

        // Try to decode the scenario root to pick up global overrides (images handled by run())
        let scenariosData = try Data(contentsOf: URL(fileURLWithPath: scenarioConfig))
        let decoder = JSONDecoder()
        if let root = try? decoder.decode(ScenarioRoot.self, from: scenariosData) {
            orchestrator.applyGlobalOverrides(images: nil, timeout: root.timeout, retries: root.retries, repetitions: root.repetitions, persistence: root.persistence, collectMetrics: root.collectMetrics, testSuiteName: root.testSuiteName)
        }

        // Ignore default signal handling so we can manage it manually
        signal(SIGINT, SIG_IGN)
        signal(SIGTERM, SIG_IGN)

        // State tracking
        var didStartCleanup = false

        // Define signal handler
        let handleSignal: (Int32) -> Void = { signal in
            if didStartCleanup {
                // Second signal → force exit immediately
                print("\nReceived signal \(signal) again. Force quitting!")
                TestManager.exit()
            }

            // First signal → graceful cleanup
            didStartCleanup = true
            print("\nReceived signal \(signal). Performing cleanup...")

            Task {
                // Run async cleanup
                orchestrator.cleanup()

                print("Cleanup complete. Exiting gracefully.")
                TestManager.exit()
            }
        }

        // Set up signal sources
        let signals = [SIGINT, SIGTERM]
        var sources: [DispatchSourceSignal] = []

        for sig in signals {
            let source = DispatchSource.makeSignalSource(signal: sig, queue: .main)
            source.setEventHandler { handleSignal(sig) }
            source.activate()
            sources.append(source)
        }

        let results = try await orchestrator.run(using: URL(fileURLWithPath: scenarioConfig), images: nil)
        try writeReport(results: results)
    }

    private func writeReport(results: [TestResult]) throws {
        let dir = URL(fileURLWithPath: "test-output")
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let df = DateFormatter()
        df.dateFormat = "yyyyMMdd_HHmmss"
        let path = dir.appendingPathComponent("raft_swarm_test_report_\(df.string(from: Date())).txt")

        var text = "Raft Swarm Test Suite Report\nGenerated on: \(Date())\n\n"
        let passed = results.filter(\.success)
        text += "Summary: \(passed.count) / \(results.count) tests passed.\n\n"

        let failed = results.filter { !$0.success }
        if !failed.isEmpty {
            text += "--- FAILED TESTS ---\n"
            for r in failed {
                text += "Test \(r.testNumber): FAILED\n"
                text += "  Parameters: \(describe(r.parameters))\n"
                text += String(format: "  Duration: %.2fs\n", r.duration)
                for rep in r.repetitions {
                    text += "    Repetition \(rep.repetition): \(rep.status.rawValue.uppercased())\n"
                }
                text += "\n"
            }
        }

        if !passed.isEmpty {
            text += "--- PASSED TESTS ---\n"
            for r in passed {
                text += "Test \(r.testNumber): PASSED\n"
                text += "  Parameters: \(describe(r.parameters))\n"
                text += String(format: "  Duration: %.2fs\n", r.duration)
                for rep in r.repetitions {
                    text += "    Repetition \(rep.repetition): \(rep.status.rawValue.uppercased())\n"
                }
                text += "\n"
            }
        }

        try text.write(to: path, atomically: true, encoding: .utf8)
        print("Report written to \(path.path)")
    }

    private func describe(_ p: TestCombination) -> String {
        "{image: \(p.image), compaction: \(p.compactionThreshold), peers: \(p.peers), ops: \(p.operations), conc: \(p.concurrency), cpu: \(p.cpuLimit ?? "nil"), mem: \(p.memoryLimit ?? "nil"), persistence: \(p.persistence), scenario: \(p.scenarioName ?? "nil"), DAS: \(p.useDistributedActorSystem)}"
    }
}
