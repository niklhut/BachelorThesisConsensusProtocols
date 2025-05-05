import DistributedCluster

actor ReplicationTracker {
    let totalPeers: Int
    let majority: Int
    var successful: Set<ClusterSystem.ActorID> = []
    var continuations: [CheckedContinuation<Void, Never>] = []

    init(peerCount: Int, majority: Int) {
        self.totalPeers = peerCount
        self.majority = majority
    }

    func markSuccess(id: ClusterSystem.ActorID) {
        guard !successful.contains(id) else { return }
        successful.insert(id)
        if successful.count >= majority {
            for cont in continuations {
                cont.resume()
            }
            continuations.removeAll()
        }
    }

    func waitForMajority() async {
        if successful.count >= majority {
            return
        }

        await withCheckedContinuation { cont in
            if successful.count >= majority {
                cont.resume()
            } else {
                continuations.append(cont)
            }
        }
    }

    func isSuccessful(id: ClusterSystem.ActorID) -> Bool {
        return successful.contains(id)
    }

    func getSuccessfulPeers() -> Set<ClusterSystem.ActorID> {
        return successful
    }
}
