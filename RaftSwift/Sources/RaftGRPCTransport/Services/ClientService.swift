import Foundation
import GRPCCore
import RaftCore
import SwiftProtobuf

struct ClientService: Raft_RaftClient.SimpleServiceProtocol {
    let node: any RaftNodeProtocol

    init(node: any RaftNodeProtocol) {
        self.node = node
    }

    func put(request: Raft_PutRequest, context: ServerContext) async throws -> Raft_PutResponse {
        let response = try await node.put(request: PutRequest(key: request.key, value: request.value))

        return .with { grpcResponse in
            grpcResponse.success = response.success
            if let leaderHint = response.leaderHint {
                grpcResponse.leaderHint = leaderHint.toGRPC()
            }
        }
    }

    func get(request: Raft_GetRequest, context: ServerContext) async throws -> Raft_GetResponse {
        let response = await node.get(request: GetRequest(key: request.key))

        return .with { grpcResponse in
            if let value = response.value {
                grpcResponse.value = value
            }
            if let leaderHint = response.leaderHint {
                grpcResponse.leaderHint = leaderHint.toGRPC()
            }
        }
    }

    func getDebug(request: Raft_GetRequest, context: ServerContext) async throws -> Raft_GetResponse {
        let response = await node.getDebug(request: GetRequest(key: request.key))

        return .with { grpcResponse in
            if let value = response.value {
                grpcResponse.value = value
            }
            if let leaderHint = response.leaderHint {
                grpcResponse.leaderHint = leaderHint.toGRPC()
            }
        }
    }

    func getServerState(request: Google_Protobuf_Empty, context: ServerContext) async throws -> Raft_ServerStateResponse {
        let response = await node.getState()

        return .with { grpcResponse in
            grpcResponse.id = UInt32(response.id)
            grpcResponse.state = response.state.toGRPC()
        }
    }

    func getServerTerm(request: Google_Protobuf_Empty, context: ServerContext) async throws -> Raft_ServerTermResponse {
        let response = await node.getTerm()

        return .with { grpcResponse in
            grpcResponse.term = UInt64(response.term)
            grpcResponse.id = UInt32(response.id)
        }
    }

    func getDiagnostics(request: Raft_DiagnosticsRequest, context: ServerContext) async throws -> Raft_DiagnosticsResponse {
        let response: DiagnosticsResponse = await node.getDiagnostics(request: DiagnosticsRequest(
            start: request.startTime.date,
            end: request.endTime.date,
        ))

        return .with { grpcResponse in
            grpcResponse.id = UInt32(response.id)
            grpcResponse.implementation = response.implementation
            grpcResponse.version = response.version
            grpcResponse.compactionThreshold = UInt32(response.compactionThreshold)
            grpcResponse.metrics = response.metrics?.toGRPC() ?? []
        }
    }
}
