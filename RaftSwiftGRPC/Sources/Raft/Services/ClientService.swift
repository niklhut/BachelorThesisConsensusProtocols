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
}
