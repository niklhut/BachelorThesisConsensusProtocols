import Distributed
import DistributedCluster
import Logging

protocol PeerConnectable {
    /// The list of peers to connect to.
    var peers: [PeerConfig] { get }

    /// The maximum number of retry attempts for connecting to peers.
    var maxRetries: Int { get }

    /// The delay between retry attempts in seconds.
    var retryDelay: Double { get }

    /// The logger for the node.
    var logger: Logger { get }

    /// Connects to a peer with retry attempts.
    ///
    /// - Parameters:
    ///   - system: The cluster system to connect with the peer.
    ///   - peer: The peer to connect to.
    /// - Throws: An error if the connection fails.
    func connectToPeerWithRetry(system: ClusterSystem, peer: PeerConfig) async throws

    /// Connects to all peers with retry attempts.
    ///
    /// - Parameter system: The cluster system to connect with the peers.
    /// - Throws: An error if the connection fails.
    func connectToPeers(system: ClusterSystem) async throws
}

extension PeerConnectable {
    func connectToPeerWithRetry(system: ClusterSystem, peer: PeerConfig) async throws {
        var retryCount = 0
        var connected = false

        while !connected && retryCount < maxRetries {
            do {
                logger.info(
                    "Attempting to connect to peer \(peer.id) at \(peer.name):\(peer.port) (Attempt \(retryCount + 1))"
                )

                let peerAddress = Cluster.Endpoint(
                    host: peer.name,
                    port: peer.port
                )

                // Wait for the peer to join
                system.cluster.join(endpoint: peerAddress)
                try await system.cluster.waitFor(
                    peerAddress, .joining, within: Duration.seconds(retryDelay))

                logger.notice("Successfully connected to peer \(peer.id) at \(peerAddress)")
                connected = true
            } catch {
                retryCount += 1
                logger.warning(
                    "Failed to connect to peer \(peer.id): \(error.localizedDescription)")

                if retryCount >= maxRetries {
                    logger.error(
                        "Max retry attempts reached for peer \(peer.id). Shutting down local node..."
                    )
                    try system.shutdown()
                }
            }
        }
    }

    func connectToPeers(system: ClusterSystem) async throws {
        try await peers.concurrentForEach { peer in
            try await connectToPeerWithRetry(system: system, peer: peer)
        }

        logger.info("All peer connections established.")
    }
}
