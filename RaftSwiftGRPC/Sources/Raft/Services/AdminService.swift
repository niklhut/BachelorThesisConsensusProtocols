import GRPCCore
import SwiftProtobuf

struct AdminService: Raft_RaftAdmin.SimpleServiceProtocol {
    let node: any RaftNodeRPC

    init(node: any RaftNodeRPC) {
        self.node = node
    }

    func getServerState(request: Google_Protobuf_Empty, context: ServerContext) async throws -> Raft_ServerStateResponse {
        try await node.getState()
    }

    func getServerTerm(request: Google_Protobuf_Empty, context: ServerContext) async throws -> Raft_ServerTermResponse {
        try await node.getTerm()
    }
}
