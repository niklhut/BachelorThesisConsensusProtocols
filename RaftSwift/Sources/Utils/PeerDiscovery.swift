import DistributedCluster

protocol PeerDiscovery where Self: LifecycleWatch {
    var peers: Set<RaftNode> { get set }
    var listingTask: Task<Void, Never>? { get set }

    func findPeers() async
}

extension PeerDiscovery {
    /// Continuously finds all peers in the cluster.
    func findPeers() {
        guard listingTask == nil else {
            actorSystem.log.warning("Already looking for peers")
            return
        }

        listingTask = Task {
            for await peer in await actorSystem.receptionist.listing(of: .raftNode) {
                actorSystem.log.info("Found peer: \(peer)")
                peers.insert(peer)
                watchTermination(of: peer)
            }
        }
    }
}
