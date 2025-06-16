import RaftCore

extension Snapshot {
    func toGRPC() -> Raft_Snapshot {
        .with { snapshot in
            snapshot.lastIncludedIndex = UInt64(lastIncludedIndex)
            snapshot.lastIncludedTerm = UInt64(lastIncludedTerm)
            snapshot.stateMachine = stateMachine
        }
    }
}
