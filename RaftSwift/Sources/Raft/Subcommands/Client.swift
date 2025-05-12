import ArgumentParser
import Distributed
import DistributedCluster
import Foundation
import Logging

final class Client: AsyncParsableCommand, PeerConnectable {
    static let configuration = CommandConfiguration(
        commandName: "client",
        abstract: "Start a client node to test Raft consensus"
    )

    lazy var logger = Logger(label: "RaftClient")

    @Option(help: "The list of peers in the format 'id:name:port,...'.")
    var peers: [PeerConfig]

    @Option(help: "Maximum retry attempts for connecting to peers")
    var maxRetries: Int = GlobalConfig.maxRetries

    @Option(help: "Delay between retry attempts in seconds")
    var retryDelay: Double = GlobalConfig.retryDelay

    @Flag(help: "Run basic correctness test")
    var correctnessTest: Bool = false

    @Flag(help: "Run stress test")
    var stressTest: Bool = false

    @Option(help: "Number of operations for stress test")
    var operations: Int = 1000

    @Option(help: "Concurrency level for stress test")
    var concurrency: Int = 10

    func run() async throws {
        logger.info("Creating client...\nPeers: \(peers)")

        let system = await ClusterSystem("Client") { settings in
            settings.logging.baseLogger = Logger(label: "RaftClient") { label in
                ColoredConsoleLogHandler(label: label)
            }
        }

        try await connectToPeers(system: system)

        // Create the client actor
        let client = RaftClient(actorSystem: system)
        await system.receptionist.checkIn(client, with: .raftClient)

        // Start the client
        try await client.start()

        // Wait for peer discovery
        logger.info("Waiting for peer discovery...")

        var testsToRun: [RaftClient.TestType] = []
        if correctnessTest {
            testsToRun.append(.correctness)
        }

        if stressTest {
            testsToRun.append(.stress)
        }

        if !correctnessTest, !stressTest {
            logger.error("No test specified, running all tests")
            testsToRun = RaftClient.TestType.allCases
        }

        // Run the specified tests
        for test in testsToRun {
            switch test {
            case .correctness:
                logger.info("Starting correctness test...")
                let result = try await client.runCorrectnessTest()
                logger.info("Correctness test completed.")
                logger.info(
                    "Success rate: \(Double(result.successfulOperations) / Double(result.totalOperations) * 100)%"
                )

            case .stress:
                logger.info("Starting stress test...")
                let result = try await client.runStressTest(
                    operations: operations, concurrency: concurrency
                )
                logger.info("Stress test completed.")
                logger.info(
                    "Throughput: \(result.throughput) ops/sec, Avg Latency: \(result.averageLatency) ms"
                )
            }
        }

        try await client.stop()
    }
}
