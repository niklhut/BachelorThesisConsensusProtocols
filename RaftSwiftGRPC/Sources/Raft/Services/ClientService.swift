import GRPCCore
import SwiftProtobuf

struct ClientService: Raft_RaftClient.SimpleServiceProtocol {
    let node: any RaftNodeRPC

    init(node: any RaftNodeRPC) {
        self.node = node
    }

    func put(request: Raft_PutRequest, context: GRPCCore.ServerContext) async throws -> Raft_PutResponse {
        try await node.put(request: request, context: context)
    }

    func get(request: Raft_GetRequest, context: GRPCCore.ServerContext) async throws -> Raft_GetResponse {
        try await node.get(request: request, context: context)
    }

    func getDebug(request: Raft_GetRequest, context: GRPCCore.ServerContext) async throws -> Raft_GetResponse {
        try await node.getDebug(request: request, context: context)
    }

    func getServerState(request: Google_Protobuf_Empty, context: ServerContext) async throws -> Raft_ServerStateResponse {
        try await node.getState()
    }

    func getServerTerm(request: Google_Protobuf_Empty, context: ServerContext) async throws -> Raft_ServerTermResponse {
        try await node.getTerm()
    }
}
