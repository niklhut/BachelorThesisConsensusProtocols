/// Node to Client, response to ServerTermRequest
public struct ServerTermResponse: Sendable, Codable {
    /// The ID of the server
    public let id: Int

    /// The term of the server
    public let term: Int

    /// Initializes a new ServerTermResponse
    /// - Parameters:
    ///   - id: The ID of the server
    ///   - term: The term of the server
    public init(id: Int, term: Int) {
        self.id = id
        self.term = term
    }
}
