import ArgumentParser
import DistributedCluster
import Foundation
import Logging

final class Peer: AsyncParsableCommand, PeerConnectable {
    static let configuration = CommandConfiguration(
        commandName: "peer",
        abstract: "Start a peer node"
    )

    lazy var logger: Logger = Logger(label: "RaftPeer\(id)")

    @Option(help: "The ID of this server")
    var id: Int

    @Option(help: "The port to listen on for incoming connections")
    var port: Int = 10001

    @Option(help: "The list of peers in the format 'id:name:port,...'.")
    var peers: [PeerConfig]

    @Option(help: "Maximum retry attempts for connecting to peers")
    var maxRetries: Int = GlobalConfig.maxRetries

    @Option(help: "Delay between retry attempts in seconds")
    var retryDelay: Double = GlobalConfig.retryDelay

    func run() async throws {
        logger.info("Creating node...\nID: \(id)\nPort: \(port)\nPeers: \(peers)")

        let system = await ClusterSystem("Node \(id)") { settings in
            settings.bindPort = port
            settings.bindHost = "0.0.0.0"
        }
        let raftNode = RaftNode(actorSystem: system)

        logger.info("Local node started. Attempting to connect to peers...")

        try await connectToPeers(system: system)

        print("\n\nStarting raft node...")
        try await raftNode.start()
        await system.receptionist.checkIn(raftNode, with: .raftNode)

        // Keep the application running
        try await system.terminated
    }
}
