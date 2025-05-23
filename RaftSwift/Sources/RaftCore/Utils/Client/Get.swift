/// Client to Leader, used for retrieving a value by key
public struct GetRequest: Sendable, Codable {
    /// Key to retrieve
    public let key: String

    /// Initializes a new GetRequest
    /// - Parameters:
    ///   - key: Key to retrieve
    public init(key: String) {
        self.key = key
    }
}

/// Leader to Client, response to GetRequest
public struct GetResponse: Sendable, Codable {
    /// Value associated with the key, null if not found
    public let value: String?

    /// Optional leader hint if this node is not the leader
    public let leaderHint: Peer?

    /// Initializes a new GetResponse
    /// - Parameters:
    ///   - value: Value associated with the key, null if not found
    ///   - leaderHint: Optional leader hint if this node is not the leader
    public init(value: String? = nil, leaderHint: Peer? = nil) {
        self.value = value
        self.leaderHint = leaderHint
    }
}
