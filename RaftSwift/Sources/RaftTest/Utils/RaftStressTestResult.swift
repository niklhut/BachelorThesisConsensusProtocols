struct RaftStressTestResult: Codable {
    let messagesSent: Int
    let successfulMessages: Int
    let averageLatency: Double
    let averageThroughput: Double
    let totalDuration: Double
    let concurrency: Int
    let numberOfPeers: Int

    var description: String {
        """
        Stress Test Results:
        - Messages Sent: \(messagesSent)
        - Successful Messages: \(successfulMessages)
        - Average Latency: \(averageLatency) ms
        - Average Throughput: \(averageThroughput) ops/sec
        - Total Duration: \(totalDuration) seconds
        - Concurrency Level: \(concurrency)
        - Number of Peers: \(numberOfPeers)
        """
    }
}

struct RaftStressTestPayload: Codable {
    let messagesSent: Int
    let successfulMessages: Int
    let averageLatency: Double
    let averageThroughput: Double
    let totalDuration: Double
    let concurrency: Int
    let machine: String
    let numberOfPeers: Int
    let peerVersion: RaftImplementationVersion
}

struct RaftImplementationVersion: Codable {
    let implementation: String
    let version: String
}
