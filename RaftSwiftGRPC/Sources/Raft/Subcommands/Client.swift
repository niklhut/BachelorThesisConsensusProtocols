import ArgumentParser

final class Client: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "client",
        abstract: "Start a raft client node",
    )

    // MARK: - Common Options

    @Option(help: "The list of peers in the format 'id:name:port,...'.")
    var peers: [Raft_Peer]

    // MARK: - Interactive Mode

    @Flag(help: "Run in interactive mode")
    var interactive: Bool = false

    // MARK: - Stress Test

    @Flag(help: "Run stress test")
    var stressTest: Bool = false

    @Option(help: "Number of operations for stress test")
    var operations: Int = 1000

    @Option(help: "Concurrency level for stress test")
    var concurrency: Int = 10

    // MARK: - Run

    func run() async throws {
        let client = RaftClient(peers: peers)

        if interactive {
            let consoleClient = InteractiveConsoleClient(client: client)
            try await consoleClient.run()
        } else if stressTest {
            let stressTestClient = StressTestClient(client: client)
            try await stressTestClient.run(operations: operations, concurrency: concurrency)
        }
    }
}
