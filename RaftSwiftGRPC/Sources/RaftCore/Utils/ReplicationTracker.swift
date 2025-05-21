/// Tracks the replication of log entries to peers.
actor ReplicationTracker {
    /// The number of peers that need to have replicated the log entries.
    let majority: Int

    /// The set of peers that have successfully replicated the log entries.
    var successful: Set<Int> = []

    /// The set of continuations to be resumed when the majority of peers have replicated the log entries.
    var continuations: [CheckedContinuation<Void, Never>] = []

    /// Initializes a new replication tracker.
    ///
    /// - Parameter majority: The number of peers that need to have replicated the log entries.
    init(majority: Int) {
        self.majority = majority
    }

    /// Marks a peer as successful.
    ///
    /// - Parameter id: The ID of the peer that has successfully replicated the log entries.
    func markSuccess(id: Int) {
        guard !successful.contains(id) else { return }
        successful.insert(id)
        if successful.count >= majority {
            for cont in continuations {
                cont.resume()
            }
            continuations.removeAll()
        }
    }

    /// Waits for the majority of peers to have replicated the log entries.
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

    /// Returns whether a peer has successfully replicated the log entries.
    ///
    /// - Parameter id: The ID of the peer to check.
    func isSuccessful(id: Int) -> Bool {
        successful.contains(id)
    }

    /// Returns the set of peers that have successfully replicated the log entries.
    func getSuccessfulPeers() -> Set<Int> {
        successful
    }
}
