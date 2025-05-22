import DistributedCluster
import RaftCore

protocol PeerConnectable {
    /// The list of peers
    var peers: [Peer] { get }

    /// Connects to the peers
    func connectToPeers(actorSystem: ClusterSystem)
}

extension PeerConnectable {
    func connectToPeers(actorSystem: ClusterSystem) {
        let peerAddresses: [Cluster.Endpoint] = peers.map { peer in
            Cluster.Endpoint(
                host: peer.address,
                port: peer.port
            )
        }

        for peerAddress in peerAddresses {
            actorSystem.cluster.join(endpoint: peerAddress)
        }
    }
}
