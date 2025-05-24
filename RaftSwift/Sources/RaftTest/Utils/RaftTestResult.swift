import Foundation

/// Test result for individual test cases
public struct RaftTestResult: Sendable {
    public let testName: String
    public let success: Bool
    public let error: Error?
    public let duration: TimeInterval
    public let details: String?

    public init(testName: String, success: Bool, error: Error? = nil, duration: TimeInterval, details: String? = nil) {
        self.testName = testName
        self.success = success
        self.error = error
        self.duration = duration
        self.details = details
    }
}
