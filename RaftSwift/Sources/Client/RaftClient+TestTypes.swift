extension RaftClient {
    enum TestError: Error {
        case noLeaderAvailable
        case leaderCrashed
        case leaderChanged
    }

    struct TestResult: Codable, Equatable {
        /// The total number of operations performed.
        var totalOperations: Int
        /// The number of operations that were successful.
        var successfulOperations: Int
        /// The number of operations that failed.
        var failedOperations: Int
        /// The average latency of the operations in milliseconds.
        var averageLatency: Double
        /// The throughput of the operations in operations per second.
        var throughput: Double
        /// The duration of the test in seconds.
        var testDuration: Double
    }

    enum TestType: CaseIterable {
        case correctness
        case stress
    }
}
