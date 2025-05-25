import RaftCore

public actor NetworkPartitionController<Transport: RaftPartitionTransport> {
    var peers: [Peer]
    var partitioned: Bool = false
    var client: RaftClient<Transport>

    init(peers: [Peer], client: RaftClient<Transport>) {
        self.peers = peers
        self.client = client
    }

    func createPartition(group1: [Peer], group2: [Peer]) async throws {
        guard !partitioned else {
            throw RaftPartitionError.partitionAlreadyExists
        }

        partitioned = true
        try await withThrowingTaskGroup(of: Void.self) { group in
            for peer in group1 {
                group.addTask {
                    try await self.client.blockPeers(
                        BlockPeerRequest(peerIds: group2.map(\.id)),
                        at: peer,
                    )
                }
            }
            for peer in group2 {
                group.addTask {
                    try await self.client.blockPeers(
                        BlockPeerRequest(peerIds: group1.map(\.id)),
                        at: peer,
                    )
                }
            }

            try await group.waitForAll()
        }
    }

    func healPartition() async throws {
        try await withThrowingTaskGroup(of: Void.self) { group in
            for peer in peers {
                group.addTask {
                    try await self.client.clearBlockedPeers(
                        at: peer,
                    )
                }
            }

            try await group.waitForAll()
        }

        partitioned = false
    }
}
