import Foundation

public struct MetricsSample: Codable, Sendable {
    /// The timestamp of the sample
    public let timestamp: Date

    /// The CPU usage percentage
    public let cpu: Double

    /// The memory usage in MB
    public let memoryMB: Double

    /// Creates a new metric sample.
    /// - Parameters:
    ///   - timestamp: The timestamp of the sample
    ///   - cpu: The CPU usage percentage
    ///   - memoryMB: The memory usage in MB
    public init(timestamp: Date, cpu: Double, memoryMB: Double) {
        self.timestamp = timestamp
        self.cpu = cpu
        self.memoryMB = memoryMB
    }
}
