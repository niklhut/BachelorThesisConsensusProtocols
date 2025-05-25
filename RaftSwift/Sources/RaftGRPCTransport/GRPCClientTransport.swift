import RaftCore
import SwiftProtobuf

/// GRPC implementation of the RaftClientTransport protocol.
final class GRPCClientTransport: RaftClientTransport {
    /// The client pool to use for communication with the server.
    let clientPool: GRPCClientPool

    /// Initializes a new instance of the GRPCClientTransport class.
    /// - Parameters:
    ///   - clientPool: The client pool to use for communication with the server.
    init(clientPool: GRPCClientPool) {
        self.clientPool = clientPool
    }

    func get(
        _ request: GetRequest,
        from peer: Peer,
        isolation: isolated any Actor
    ) async throws -> GetResponse {
        let client = try await clientPool.client(for: peer)
        let peerClient = Raft_RaftClient.Client(wrapping: client)

        let response = try await peerClient.get(.with { grpcRequest in
            grpcRequest.key = request.key
        })

        let leaderHint: Peer? = if response.hasLeaderHint {
            try .fromGRPC(response.leaderHint)
        } else {
            nil
        }

        return GetResponse(
            value: response.hasValue ? response.value : nil,
            leaderHint: leaderHint
        )
    }

    func getDebug(
        _ request: GetRequest,
        from peer: Peer,
        isolation: isolated any Actor
    ) async throws -> GetResponse {
        let client = try await clientPool.client(for: peer)
        let peerClient = Raft_RaftClient.Client(wrapping: client)

        let response = try await peerClient.getDebug(.with { grpcRequest in
            grpcRequest.key = request.key
        })

        let leaderHint: Peer? = if response.hasLeaderHint {
            try .fromGRPC(response.leaderHint)
        } else {
            nil
        }

        return GetResponse(
            value: response.hasValue ? response.value : nil,
            leaderHint: leaderHint
        )
    }

    func put(
        _ request: PutRequest,
        to peer: Peer,
        isolation: isolated any Actor
    ) async throws -> PutResponse {
        let client = try await clientPool.client(for: peer)
        let peerClient = Raft_RaftClient.Client(wrapping: client)

        let response = try await peerClient.put(.with { grpcRequest in
            grpcRequest.key = request.key
            if let value = request.value {
                grpcRequest.value = value
            }
        })

        let leaderHint: Peer? = if response.hasLeaderHint {
            try .fromGRPC(response.leaderHint)
        } else {
            nil
        }

        return PutResponse(
            success: response.success,
            leaderHint: leaderHint
        )
    }

    func getServerState(
        of peer: Peer,
        isolation: isolated any Actor
    ) async throws -> ServerStateResponse {
        let client = try await clientPool.client(for: peer)
        let peerClient = Raft_RaftClient.Client(wrapping: client)

        let response = try await peerClient.getServerState(Google_Protobuf_Empty())

        return try ServerStateResponse(
            id: Int(response.id),
            state: .fromGRPC(response.state)
        )
    }

    func getTerm(
        of peer: Peer,
        isolation: isolated any Actor
    ) async throws -> ServerTermResponse {
        let client = try await clientPool.client(for: peer)
        let peerClient = Raft_RaftClient.Client(wrapping: client)

        let response = try await peerClient.getServerTerm(Google_Protobuf_Empty())

        return ServerTermResponse(
            id: Int(response.id),
            term: Int(response.term)
        )
    }
}
