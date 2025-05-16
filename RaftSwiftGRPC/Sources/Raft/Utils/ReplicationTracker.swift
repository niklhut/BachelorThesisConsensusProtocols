actor ReplicationTracker<ID: Hashable> {
    let majority: Int
    var successful: Set<ID> = []
    var continuations: [CheckedContinuation<Void, Never>] = []

    init(majority: Int) {
        self.majority = majority
    }

    func markSuccess(id: ID) {
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

    func isSuccessful(id: ID) -> Bool {
        successful.contains(id)
    }

    func getSuccessfulPeers() -> Set<ID> {
        successful
    }
}
