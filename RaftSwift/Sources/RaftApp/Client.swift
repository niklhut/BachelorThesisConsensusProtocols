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

    @Flag(help: "Skip sanity check after stress test")
    var skipSanityCheck: Bool = true

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

        if interactive {
            try await client.runInteractiveClient()
        } else if tests {
            try await client.runFunctionalityTests()
        } else if stressTest {
            try await client.runStressTest(operations: operations, concurrency: concurrency, testSuiteName: testSuite, skipSanityCheck: skipSanityCheck)
        }
    }
}
