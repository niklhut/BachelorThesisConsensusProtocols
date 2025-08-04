import GRPCCore
import RaftCore

struct PeerService: Raft_RaftPeer.SimpleServiceProtocol {
    let node: any RaftNodeProtocol

    init(node: any RaftNodeProtocol) {
        self.node = node
    }

    func requestVote(request: Raft_RequestVoteRequest, context: ServerContext) async throws -> Raft_RequestVoteResponse {
        let response = await node.requestVote(request: RequestVoteRequest(
            term: Int(request.term),
            candidateID: Int(request.candidateID),
            lastLogIndex: Int(request.lastLogIndex),
            lastLogTerm: Int(request.lastLogTerm),
        ))

        return .with { grpcResponse in
            grpcResponse.term = UInt64(response.term)
            grpcResponse.voteGranted = response.voteGranted
        }
    }

    func appendEntries(request: Raft_AppendEntriesRequest, context: ServerContext) async throws -> Raft_AppendEntriesResponse {
        let response = await node.appendEntries(request: AppendEntriesRequest(
            term: Int(request.term),
            leaderID: Int(request.leaderID),
            prevLogIndex: Int(request.prevLogIndex),
            prevLogTerm: Int(request.prevLogTerm),
            entries: request.entries.map { LogEntry(term: Int($0.term), key: $0.key, value: $0.value) },
            leaderCommit: Int(request.leaderCommit),
        ))

        return .with { grpcResponse in
            grpcResponse.term = UInt64(response.term)
            grpcResponse.success = response.success
        }
    }

    func installSnapshot(request: Raft_InstallSnapshotRequest, context: ServerContext) async throws -> Raft_InstallSnapshotResponse {
        let response = try await node.installSnapshot(request: InstallSnapshotRequest(
            term: Int(request.term),
            leaderID: Int(request.leaderID),
            snapshot: Snapshot(
                lastIncludedIndex: Int(request.snapshot.lastIncludedIndex),
                lastIncludedTerm: Int(request.snapshot.lastIncludedTerm),
                stateMachine: request.snapshot.stateMachine,
            ),
        ))

        return .with { grpcResponse in
            grpcResponse.term = UInt64(response.term)
        }
    }
}
