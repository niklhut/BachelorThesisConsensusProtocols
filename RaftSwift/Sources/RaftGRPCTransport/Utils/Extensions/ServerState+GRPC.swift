import RaftCore

extension ServerState {
    func toGRPC() -> Raft_ServerState {
        switch self {
        case .follower:
            .follower
        case .candidate:
            .candidate
        case .leader:
            .leader
        }
    }

    static func fromGRPC(_ state: Raft_ServerState) throws -> ServerState {
        switch state {
        case .follower:
            .follower
        case .candidate:
            .candidate
        case .leader:
            .leader
        default:
            throw RaftGRPCError.invalidServerState
        }
    }
}
