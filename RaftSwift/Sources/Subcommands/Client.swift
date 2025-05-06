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

    func run() async throws {
        logger.info("Creating client...\nPeers: \(peers)")

        let system = await ClusterSystem("Client")

        try await connectToPeers(system: system)

        // Create the client actor
        let client = RaftClient(actorSystem: system)
        await system.receptionist.checkIn(client, with: .raftClient)

        // Start the client
        try await client.start()

        // Wait for peer discovery
        logger.info("Waiting for peer discovery...")
        try await Task.sleep(for: .seconds(5))

        var testsToRun: Set<TestType> = []
        if correctnessTest {
            testsToRun.insert(.correctness)
        }

        // Run the specified tests
        for test in testsToRun {
            switch test {
            case .correctness:
                logger.info("Starting correctness test...")
                let result = try await client.runCorrectnessTest()
                logger.info("Correctness test completed: \(result.description)")
                logger.info(
                    "Success rate: \(Double(result.successfulOperations) / Double(result.totalOperations) * 100)%"
                )
            }
        }

        try await client.stop()
    }
}
