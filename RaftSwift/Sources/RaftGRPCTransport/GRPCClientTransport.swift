import GRPCNIOTransportHTTP2
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

    func resetClients() async throws {
        try await clientPool.reset()
    }

    func runWithClientRetry<T: Sendable>(
        peer: Peer,
        retryCount: Int = 5,
        isolation: isolated any Actor,
        action: (_ client: Raft_RaftClient.Client<HTTP2ClientTransport.Posix>) async throws -> T,
    ) async throws -> T {
        var result: T?

        for _ in 0 ..< retryCount {
            do {
                let client = try await clientPool.client(for: peer)
                let peerClient = Raft_RaftClient.Client(wrapping: client)
                result = try await action(peerClient)
                break
            } catch {
                try await clientPool.reset()
            }
        }

        return result!
    }

    // MARK: - RaftClientTransport

    func get(
        _ request: GetRequest,
        from peer: Peer,
        isolation: isolated any Actor,
    ) async throws -> GetResponse {
        try await runWithClientRetry(peer: peer, isolation: isolation) { client in
            let response = try await client.get(.with { grpcRequest in
                grpcRequest.key = request.key
            })

            let leaderHint: Peer? = if response.hasLeaderHint {
                try .fromGRPC(response.leaderHint)
            } else {
                nil
            }

            return GetResponse(
                value: response.hasValue ? response.value : nil,
                leaderHint: leaderHint,
            )
        }
    }

    func getDebug(
        _ request: GetRequest,
        from peer: Peer,
        isolation: isolated any Actor,
    ) async throws -> GetResponse {
        try await runWithClientRetry(peer: peer, isolation: isolation) { client in
            let response = try await client.getDebug(.with { grpcRequest in
                grpcRequest.key = request.key
            })
            let leaderHint: Peer? = if response.hasLeaderHint {
                try .fromGRPC(response.leaderHint)
            } else {
                nil
            }

            return GetResponse(
                value: response.hasValue ? response.value : nil,
                leaderHint: leaderHint,
            )
        }
    }

    func put(
        _ request: PutRequest,
        to peer: Peer,
        isolation: isolated any Actor,
    ) async throws -> PutResponse {
        try await runWithClientRetry(peer: peer, isolation: isolation) { client in
            let response = try await client.put(.with { grpcRequest in
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
                leaderHint: leaderHint,
            )
        }
    }

    func getServerState(
        of peer: Peer,
        isolation: isolated any Actor,
    ) async throws -> ServerStateResponse {
        try await runWithClientRetry(peer: peer, isolation: isolation) { client in
            let response = try await client.getServerState(Google_Protobuf_Empty())

            return try ServerStateResponse(
                id: Int(response.id),
                state: .fromGRPC(response.state),
            )
        }
    }

    func getTerm(
        of peer: Peer,
        isolation: isolated any Actor,
    ) async throws -> ServerTermResponse {
        try await runWithClientRetry(peer: peer, isolation: isolation) { client in
            let response = try await client.getServerTerm(Google_Protobuf_Empty())

            return ServerTermResponse(
                id: Int(response.id),
                term: Int(response.term),
            )
        }
    }

    func getDiagnostics(
        _ request: DiagnosticsRequest,
        of peer: Peer,
        isolation: isolated any Actor,
    ) async throws -> DiagnosticsResponse {
        try await runWithClientRetry(peer: peer, isolation: isolation) { client in
            let response = try await client.getDiagnostics(.with { grpcRequest in
                grpcRequest.startTime = request.start.toGRPC()
                grpcRequest.endTime = request.end.toGRPC()
            })

            return DiagnosticsResponse(
                id: Int(response.id),
                implementation: response.implementation + " (GRPC)",
                version: response.version,
                compactionThreshold: Int(response.compactionThreshold),
                metrics: [MetricsSample].fromGRPC(response.metrics),
            )
        }
    }
}
