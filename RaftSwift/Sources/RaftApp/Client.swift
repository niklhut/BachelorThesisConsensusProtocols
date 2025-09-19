import ArgumentParser
import RaftCore
import RaftDistributedActorsTransport
import RaftGRPCTransport
import RaftTest

final class Client: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "client",
        abstract: "Start a raft client node",
    )

    // MARK: - Common Options

    @Option(help: "The list of peers in the format 'id:name:port,...'.")
    var peers: [RaftCore.Peer]

    // MARK: - Interactive Mode

    @Flag(help: "Run in interactive mode")
    var interactive: Bool = false

    // MARK: - Functionality Tests

    @Flag(help: "Run functionality tests")
    var tests: Bool = false

    // MARK: - Stress Test

    @Flag(help: "Run stress test")
    var stressTest: Bool = false

    @Option(help: "Number of operations for stress test")
    var operations: Int = 1000

    @Option(help: "Concurrency level for stress test")
    var concurrency: Int = 10

    @Option(help: "Test suite name")
    var testSuite: String = ""

    @Option(help: "Timeout for stress test in seconds")
    var timeout: Double = 300

    @Option(name: .customLong("duration-seconds"), help: "Fixed duration for stress test in seconds (overrides operations count to run by time)")
    var durationSeconds: Int? = nil

    @Flag(help: "Skip sanity check after stress test")
    var skipSanityCheck: Bool = true

    @Option(help: "The number of CPU cores available to each node")
    var cpuCores: Double? = nil

    @Option(help: "The amount of memory (in GB) available to each node")
    var memory: Double? = nil

    @Option(help: "Payload size for each message (bytes). Accepts plain bytes (e.g. 16), or human-readable sizes like 16B, 1KB, 64KB, 1MB.")
    var payloadSize: String? = nil

    // MARK: - Transport

    @Flag(help: "Use Distributed Actor System for transport")
    var useDistributedActorSystem: Bool = false

    // MARK: - Run

    func run() async throws {
        let client: any RaftTestApplication = if useDistributedActorSystem {
            RaftDistributedActorClient(peers: peers)
        } else {
            RaftGRPCClient(peers: peers)
        }

        let payloadSizeBytes = Self.parseSizeToBytes(payloadSize)

        if interactive {
            try await client.runInteractiveClient()
        } else if tests {
            try await client.runFunctionalityTests()
        } else if stressTest {
            try await client.runStressTest(
                operations: operations,
                concurrency: concurrency,
                testSuiteName: testSuite,
                timeout: timeout,
                durationSeconds: durationSeconds,
                cpuCores: cpuCores,
                memory: memory,
                payloadSizeBytes: payloadSizeBytes ?? 55, // default to 55 bytes if not specified
                skipSanityCheck: skipSanityCheck,
            )
        }
    }

    private static func parseSizeToBytes(_ s: String?) -> Int? {
        guard let s, !s.isEmpty else { return nil }
        let t = s.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
        // Exact bytes
        if let n = Int(t) { return n }
        if t.hasSuffix("B") {
            let num = String(t.dropLast())
            if let n = Int(num) { return n }
        }
        if t.hasSuffix("KB") {
            let num = String(t.dropLast(2))
            if let n = Double(num) { return Int(n * 1024.0) }
        }
        if t.hasSuffix("MB") {
            let num = String(t.dropLast(2))
            if let n = Double(num) { return Int(n * 1024.0 * 1024.0) }
        }
        if t.hasSuffix("GB") {
            let num = String(t.dropLast(2))
            if let n = Double(num) { return Int(n * 1024.0 * 1024.0 * 1024.0) }
        }
        return nil
    }
}
