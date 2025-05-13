import GRPCCore

struct PeerService: Raft_RaftPeer.ServiceProtocol {
    func requestVote(request: GRPCCore.ServerRequest<Raft_RequestVoteRequest>, context: GRPCCore.ServerContext) async throws -> GRPCCore.ServerResponse<Raft_RequestVoteResponse> {
        .init(error: .init(status: Status(code: .unimplemented, message: "Not implemented"))!)
    }

    func appendEntries(request: GRPCCore.ServerRequest<Raft_AppendEntriesRequest>, context: GRPCCore.ServerContext) async throws -> GRPCCore.ServerResponse<Raft_AppendEntriesResponse> {
        .init(error: .init(status: Status(code: .unimplemented, message: "Not implemented"))!)
    }

    func installSnapshot(request: GRPCCore.ServerRequest<Raft_InstallSnapshotRequest>, context: GRPCCore.ServerContext) async throws -> GRPCCore.ServerResponse<Raft_InstallSnapshotResponse> {
        .init(error: .init(status: Status(code: .unimplemented, message: "Not implemented"))!)
    }
}
