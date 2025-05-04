import ArgumentParser
import Distributed
import DistributedCluster
import Logging

final class Client: AsyncParsableCommand, PeerConnectable {
    static let configuration = CommandConfiguration(
        commandName: "client",
        abstract: "Start a client node"
    )

    lazy var logger = Logger(label: "RaftClient")

    @Option(help: "The list of peers in the format 'id:name:port,...'.")
    var peers: [PeerConfig]

    @Option(help: "Maximum retry attempts for connecting to peers")
    var maxRetries: Int = GlobalConfig.maxRetries

    @Option(help: "Delay between retry attempts in seconds")
    var retryDelay: Double = GlobalConfig.retryDelay

    func run() async throws {
        logger.info("Creating client...\nPeers: \(peers)")

        let system = await ClusterSystem("Client") { settings in
            settings.bindPort = 0  // Random port since we don't need to listen for incoming connections
            settings.bindHost = "0.0.0.0"
        }
        
        try await connectToPeers(system: system)

        // TODO: Client setup and benchmark
    }
}
