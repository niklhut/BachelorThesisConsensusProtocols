import RaftCore

/// GRPC implementation of the RaftNodeTransport protocol.
final class GRPCNodeTransport: RaftNodeTransport {
    /// The client pool to use for communication with the server.
    let clientPool: GRPCClientPool

    /// Initializes a new instance of the GRPCNodeTransport class.
    /// - Parameters:
    ///   - clientPool: The client pool to use for communication with the server.
    init(clientPool: GRPCClientPool) {
        self.clientPool = clientPool
    }

    func appendEntries(
        _ request: AppendEntriesRequest,
        to peer: Peer,
        isolation: isolated any Actor,
    ) async throws -> AppendEntriesResponse {
        let client = try await clientPool.client(for: peer)
        let peerClient = Raft_RaftPeer.Client(wrapping: client)

        let response = try await peerClient.appendEntries(.with { grpcRequest in
            grpcRequest.term = UInt64(request.term)
            grpcRequest.leaderID = UInt32(request.leaderID)
            grpcRequest.prevLogIndex = UInt64(request.prevLogIndex)
            grpcRequest.prevLogTerm = UInt64(request.prevLogTerm)
            grpcRequest.entries = request.entries.toGRPC()
            grpcRequest.leaderCommit = UInt64(request.leaderCommit)
        })

        return AppendEntriesResponse(
            term: Int(response.term),
            success: response.success,
        )
    }

    func requestVote(
        _ request: RequestVoteRequest,
        to peer: Peer,
        isolation: isolated any Actor,
    ) async throws -> RequestVoteResponse {
        let client = try await clientPool.client(for: peer)
        let peerClient = Raft_RaftPeer.Client(wrapping: client)

        let response = try await peerClient.requestVote(.with { grpcRequest in
            grpcRequest.term = UInt64(request.term)
            grpcRequest.candidateID = UInt32(request.candidateID)
            grpcRequest.lastLogIndex = UInt64(request.lastLogIndex)
            grpcRequest.lastLogTerm = UInt64(request.lastLogTerm)
        })

        return RequestVoteResponse(
            term: Int(response.term),
            voteGranted: response.voteGranted,
        )
    }

    func installSnapshot(
        _ request: InstallSnapshotRequest,
        on peer: Peer,
        isolation: isolated any Actor,
    ) async throws -> InstallSnapshotResponse {
        let client = try await clientPool.client(for: peer)
        let peerClient = Raft_RaftPeer.Client(wrapping: client)

        let response = try await peerClient.installSnapshot(.with { grpcRequest in
            grpcRequest.term = UInt64(request.term)
            grpcRequest.leaderID = UInt32(request.leaderID)
            grpcRequest.snapshot = request.snapshot.toGRPC()
        })

        return InstallSnapshotResponse(
            term: Int(response.term),
        )
    }
}
