import GRPCCore
import GRPCNIOTransportHTTP2
import Logging

actor RaftClient {
    let logger = Logger(label: "raft.RaftClient")

    let peers: [Raft_Peer]
    let clientPool = GRPCClientPool()

    init(peers: [Raft_Peer]) {
        self.peers = peers
    }

    // MARK: - RPC Calls

    func put(request: Raft_PutRequest, to peer: Raft_Peer? = nil) async throws -> Raft_PutResponse {
        let peer = if let peer {
            peer
        } else {
            try await findLeader()
        }

        return try await withClient(peer: peer) { client in
            try await client.put(request)
        }
    }

    func get(request: Raft_GetRequest, to peer: Raft_Peer? = nil) async throws -> Raft_GetResponse {
        let peer = if let peer {
            peer
        } else {
            try await findLeader()
        }

        return try await withClient(peer: peer) { client in
            try await client.get(request)
        }
    }

    func getDebug(request: Raft_GetRequest, to peer: Raft_Peer) async throws -> Raft_GetResponse {
        try await withClient(peer: peer) { client in
            try await client.getDebug(request)
        }
    }

    // MARK: - Helpers:

    /// Finds the leader node.
    ///
    /// - Parameter excluding: The index of the node to exclude from the search.
    /// - Throws: An error if no leader is found.
    /// - Returns: The leader node.
    func findLeader(excluding: Int? = nil) async throws -> Raft_Peer {
        var leader: Raft_Peer?

        for (index, peer) in peers.enumerated() where excluding != index {
            let response = try await withClient(peer: peer) { client in
                try await client.getServerState(.init())
            }
            if response.state == .leader {
                leader = peer
                break
            }
        }

        guard let leader else {
            throw RaftClientError.noLeaderAvailable
        }

        return leader
    }

    /// Executes a block of code with a gRPC client for a specific peer.
    ///
    /// - Parameters:
    ///   - peer: The peer to execute the block for.
    ///   - body: The block to execute.
    /// - Throws: Any errors thrown by the block.
    /// - Returns: The result of the block.
    private func withClient<T: Sendable>(peer: Raft_Peer, _ body: @Sendable @escaping (_ client: Raft_RaftClient.Client<HTTP2ClientTransport.Posix>) async throws -> T) async throws -> T {
        let client = try await clientPool.client(for: peer)
        let peerClient = Raft_RaftClient.Client(wrapping: client)
        return try await body(peerClient)
    }
}
