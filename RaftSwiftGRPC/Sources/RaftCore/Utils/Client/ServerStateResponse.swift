/// Node to Client, response to ServerStateRequest
public struct ServerStateResponse: Sendable {
    /// The ID of the server
    public let id: Int

    /// The state of the server
    public let state: ServerState

    /// Initializes a new ServerStateResponse
    /// - Parameters:
    ///   - id: The ID of the server
    ///   - state: The state of the server
    public init(id: Int, state: ServerState) {
        self.id = id
        self.state = state
    }
}
