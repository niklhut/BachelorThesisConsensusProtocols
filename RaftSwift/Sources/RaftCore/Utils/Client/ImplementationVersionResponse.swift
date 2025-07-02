/// Node to Client, response to GetDiagnosticsRequest
public struct DiagnosticsResponse: Sendable, Codable {
    /// The ID of the server
    public let id: Int

    /// The implementation version of the server
    public let implementation: String

    /// The version of the Raft implementation
    public let version: String

    /// The compaction threshold of the server
    public let compactionThreshold: Int

    /// Initializes a new DiagnosticsResponse
    /// - Parameters:
    ///   - id: The ID of the server
    ///   - implementation: The implementation version of the server
    ///   - version: The version of the Raft implementation
    ///   - compactionThreshold: The compaction threshold of the server
    public init(id: Int, implementation: String, version: String, compactionThreshold: Int) {
        self.id = id
        self.implementation = implementation
        self.version = version
        self.compactionThreshold = compactionThreshold
    }
}
