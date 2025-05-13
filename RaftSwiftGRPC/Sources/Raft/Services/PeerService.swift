import GRPCCore

struct PeerService: Raft_RaftPeer.SimpleServiceProtocol {
    let node: any RaftNodeRPC

    init(node: any RaftNodeRPC) {
        self.node = node
    }

    func requestVote(request: Raft_RequestVoteRequest, context: GRPCCore.ServerContext) async throws -> Raft_RequestVoteResponse {
        try await node.requestVote(request: request, context: context)
    }

    func appendEntries(request: Raft_AppendEntriesRequest, context: GRPCCore.ServerContext) async throws -> Raft_AppendEntriesResponse {
        try await node.appendEntries(request: request, context: context)
    }

    func installSnapshot(request: Raft_InstallSnapshotRequest, context: GRPCCore.ServerContext) async throws -> Raft_InstallSnapshotResponse {
        try await node.installSnapshot(request: request, context: context)
    }
}
