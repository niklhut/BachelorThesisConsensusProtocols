import Foundation

/// Test suite results
public struct RaftTestSuiteResult: Sendable {
    public let results: [RaftTestResult]
    public let totalDuration: TimeInterval

    public var passedTests: [RaftTestResult] { results.filter(\.success) }
    public var failedTests: [RaftTestResult] { results.filter { !$0.success } }
    public var successRate: Double {
        guard !results.isEmpty else { return 0 }
        return Double(passedTests.count) / Double(results.count)
    }

    public init(results: [RaftTestResult], totalDuration: TimeInterval) {
        self.results = results
        self.totalDuration = totalDuration
    }
}
