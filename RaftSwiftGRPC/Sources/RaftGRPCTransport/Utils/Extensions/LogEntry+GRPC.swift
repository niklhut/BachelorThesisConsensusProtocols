import RaftCore

extension LogEntry {
    func toGRPC() -> Raft_LogEntry {
        .with { entry in
            entry.term = UInt64(term)
            if let key {
                entry.key = key
            }
            if let value {
                entry.value = value
            }
        }
    }
}

extension [LogEntry] {
    func toGRPC() -> [Raft_LogEntry] {
        map { $0.toGRPC() }
    }
}
