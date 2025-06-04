struct RaftStressTestResult: Codable {
    let messagesSent: Int
    let successfulMessages: Int
    let averageLatency: Double
    let averageThroughput: Double
    let totalDuration: Double
}

struct RaftStressTestPayload: Codable {
    let messagesSent: Int
    let successfulMessages: Int
    let averageLatency: Double
    let averageThroughput: Double
    let totalDuration: Double
    let machine: String
    let implementationVersion: RaftImplementationVersion
}

struct RaftImplementationVersion: Codable {
    let implementation: String
    let version: String
}
