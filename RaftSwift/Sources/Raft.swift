@_exported import CollectionConcurrencyKit
import ArgumentParser
import DistributedCluster
import Foundation
import Logging

@main
final class Raft: AsyncParsableCommand {
    lazy var logger: Logger = Logger(label: "RaftPeer\(id)")

    @Option(help: "The ID of this server")
    var id: Int
    
    @Option(help: "The port to listen on for incoming connections")
    var port: Int = 10001
    
    @Option(help: "The list of peers in the format 'id:name:port,...'.")
    var peers: [Peer]
    
    @Option(help: "Maximum retry attempts for connecting to peers")
    var maxRetries: Int = 10
    
    @Option(help: "Delay between retry attempts in seconds")
    var retryDelay: Double = 5.0

    func run() async throws {
        logger.info("Creating node...\nID: \(id)\nPort: \(port)\nPeers: \(peers)")

        let system = await ClusterSystem("Node \(id)") { settings in
            settings.bindPort = port
            settings.bindHost = "0.0.0.0"
        }
        let raftNode = RaftNode(actorSystem: system)

        logger.info("Local node started. Attempting to connect to peers...")

        try await peers.concurrentForEach { peer in
            try await self.connectToPeerWithRetry(system: system, peer: peer)
        }
        
        logger.info("All peer connections established.")

        print("\n\nStarting raft node...")
        try await raftNode.start()
        await system.receptionist.checkIn(raftNode, with: .raftNode)

        // Keep the application running
        try await system.terminated
    }
    
    private func connectToPeerWithRetry(system: ClusterSystem, peer: Peer) async throws {
        var retryCount = 0
        var connected = false
        
        while !connected && retryCount < maxRetries {
            do {
                logger.info("Attempting to connect to peer \(peer.id) at \(peer.name):\(peer.port) (Attempt \(retryCount + 1))")
                
                let peerAddress = Cluster.Endpoint(
                    host: peer.name,
                    port: peer.port
                )
                
                // Wait for the peer to join
                system.cluster.join(endpoint: peerAddress)
                try await system.cluster.waitFor(peerAddress, .joining, within: Duration.seconds(retryDelay))

                logger.notice("Successfully connected to peer \(peer.id) at \(peerAddress)")
                connected = true
            } catch {
                retryCount += 1
                logger.warning("Failed to connect to peer \(peer.id): \(error.localizedDescription)")

                if retryCount >= maxRetries {
                    logger.error("Max retry attempts reached for peer \(peer.id). Shutting down local node...")
                    try system.shutdown()
                }
            }
        }
    }
}

