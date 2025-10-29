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

    func run() async throws {
        // Create a single timestamp when the app starts
        let dateFormatter = DateFormatter()
        dateFormatter.dateFormat = "yyyy-MM-dd-HH-mm-ss"
        let logFileName = "swarm-\(dateFormatter.string(from: Date())).log"
        let logFileURL = URL(fileURLWithPath: "test-output/\(logFileName)")

        LoggingSystem.bootstrap { label in
            var stdoutHandler = StreamLogHandler.standardOutput(label: label)
            stdoutHandler.logLevel = .debug

            let fileHandler = try! FileLogHandler(label: label, fileURL: logFileURL)
            return MultiplexLogHandler([stdoutHandler, fileHandler])
        }

        // Create orchestrator with defaults; overrides will be applied from JSON root when present.
        let orchestrator = SwarmOrchestrator()

        // Try to decode the scenario root to pick up global overrides (images handled by run())
        let scenariosData = try Data(contentsOf: URL(fileURLWithPath: scenarioConfig))
        let decoder = JSONDecoder()
        if let root = try? decoder.decode(ScenarioRoot.self, from: scenariosData) {
            orchestrator.applyGlobalOverrides(images: nil, timeout: root.timeout, retries: root.retries, repetitions: root.repetitions, persistence: root.persistence, collectMetrics: root.collectMetrics, testSuiteName: root.testSuiteName, resumeFromTestNumber: root.resumeFromTestNumber, testDurationSeconds: root.testDurationSeconds, testSwiftManualLocks: root.testSwiftManualLocks)
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

        try await orchestrator.run(using: URL(fileURLWithPath: scenarioConfig), images: nil)
    }

    private func describe(_ p: TestCombination) -> String {
        "{image: \(p.image), compaction: \(p.compactionThreshold), peers: \(p.peers), ops: \(p.operations), conc: \(p.concurrency), cpu: \(p.cpuLimit ?? "nil"), mem: \(p.memoryLimit ?? "nil"), persistence: \(p.persistence), scenario: \(p.scenarioName ?? "nil"), DAS: \(p.useDistributedActorSystem)}"
    }
}
