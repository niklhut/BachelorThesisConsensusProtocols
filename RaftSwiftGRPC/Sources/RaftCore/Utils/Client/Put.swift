/// Client to Leader, used for adding or updating a key-value pair
public struct PutRequest: Sendable {
    /// Key to store
    public let key: String

    /// Optional value (absence implies a delete)
    public let value: String?

    /// Initializes a new PutRequest
    /// - Parameters:
    ///   - key: Key to store
    ///   - value: Optional value (absence implies a delete)
    public init(key: String, value: String?) {
        self.key = key
        self.value = value
    }
}

/// Leader to Client, response to PutRequest
public struct PutResponse: Sendable {
    /// Whether the operation succeeded
    public let success: Bool

    /// Optional leader hint if request failed
    public let leaderHint: Peer?

    /// Initializes a new PutResponse
    /// - Parameters:
    ///   - success: Whether the operation succeeded
    ///   - leaderHint: Optional leader hint if request failed
    public init(success: Bool, leaderHint: Peer? = nil) {
        self.success = success
        self.leaderHint = leaderHint
    }
}
