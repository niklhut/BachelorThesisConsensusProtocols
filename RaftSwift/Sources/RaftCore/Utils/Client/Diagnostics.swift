import Foundation

/// A request for diagnostic information
public struct DiagnosticsRequest: Sendable, Codable {
    /// The start time for the metrics request
    public let start: Date

    /// The end time for the metrics request
    public let end: Date

    /// Initializes a new DiagnosticsRequest
    /// - Parameters:
    ///   - start: The start time for the metrics request
    ///   - end: The end time for the metrics request
    public init(start: Date, end: Date) {
        self.start = start
        self.end = end
    }
}

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

    /// The metrics collected by the server
    public let metrics: [MetricsSample]?

    /// Initializes a new DiagnosticsResponse
    /// - Parameters:
    ///   - id: The ID of the server
    ///   - implementation: The implementation version of the server
    ///   - version: The version of the Raft implementation
    ///   - compactionThreshold: The compaction threshold of the server
    public init(id: Int, implementation: String, version: String, compactionThreshold: Int, metrics: [MetricsSample]?) {
        self.id = id
        self.implementation = implementation
        self.version = version
        self.compactionThreshold = compactionThreshold
        self.metrics = metrics
    }
}
