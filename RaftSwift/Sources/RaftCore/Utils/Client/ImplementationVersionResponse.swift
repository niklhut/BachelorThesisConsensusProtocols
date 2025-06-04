/// Node to Client, response to GetImplementationVersionRequest
public struct ImplementationVersionResponse: Sendable, Codable {
    /// The ID of the server
    public let id: Int

    /// The implementation version of the server
    public let implementation: String

    /// The version of the Raft implementation
    public let version: String

    /// Initializes a new ImplementationVersionResponse
    /// - Parameters:
    ///   - id: The ID of the server
    ///   - implementation: The implementation version of the server
    ///   - version: The version of the Raft implementation
    public init(id: Int, implementation: String, version: String) {
        self.id = id
        self.implementation = implementation
        self.version = version
    }
}
