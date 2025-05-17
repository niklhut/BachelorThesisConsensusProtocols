extension Raft_PutRequest {
    init(key: String, value: String?) {
        self.init()
        self.key = key
        if let value {
            self.value = value
        }
    }
}

extension Raft_PutResponse {
    init(success: Bool, leaderHint: String? = nil) {
        self.init()
        self.success = success
        if let leaderHint {
            self.leaderHint = leaderHint
        }
    }
}

extension Raft_GetRequest {
    init(key: String) {
        self.init()
        self.key = key
    }
}

extension Raft_GetResponse {
    init(value: String? = nil, leaderHint: String? = nil) {
        self.init()
        if let value {
            self.value = value
        }
        if let leaderHint {
            self.leaderHint = leaderHint
        }
    }
}

extension Raft_LogEntry {
    init(term: UInt64, key: String, value: String?) {
        self.init()
        self.term = term
        self.key = key
        if let value {
            self.value = value
        }
    }
}
