import DistributedCluster

actor ReplicationTracker {
    let totalPeers: Int
    let majority: Int
    var successful: Set<ClusterSystem.ActorID> = []
    var continuation: CheckedContinuation<Void, Never>?

    init(peerCount: Int, majority: Int) {
        self.totalPeers = peerCount
        self.majority = majority
    }

    func markSuccess(id: ClusterSystem.ActorID) {
        guard !successful.contains(id) else { return }
        successful.insert(id)
        if successful.count >= majority {
            continuation?.resume()
            continuation = nil
        }
    }

    func waitForMajority() async {
        // If we have a majority, return
        if successful.count >= majority {
            return
        }

        // Otherwise wait for majority via continuation
        await withCheckedContinuation { cont in
            continuation = cont
        }
    }

    func isSuccessful(id: ClusterSystem.ActorID) -> Bool {
        return successful.contains(id)
    }

    func getSuccessfulPeers() -> Set<ClusterSystem.ActorID> {
        return successful
    }
}
