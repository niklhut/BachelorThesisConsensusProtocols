import Foundation

// MARK: - Scenario JSON Models

public struct ScenarioConfig: Decodable, Sendable {
    let name: String?
    let fixed: ScenarioAxes?
    let vary: ScenarioAxes?
}

// Optional root wrapper allowing global servers and analytics config plus scenarios list
public struct ScenarioRoot: Decodable, Sendable {
    public let servers: [ServerEntry]?
    public let analytics: AnalyticsSettings?
    public let scenarios: [ScenarioConfig]
    // optional global overrides
    public let images: [String]?
    public let timeout: Int?
    public let testDurationSeconds: Int?
    public let retries: Int?
    public let repetitions: Int?
    public let persistence: TestPersistence?
    public let collectMetrics: Bool?
    public let testSuiteName: String?
    public let resumeFromTestNumber: Int?

    enum CodingKeys: String, CodingKey {
        case servers
        case analytics
        case scenarios
        case images
        case timeout
        case testDurationSeconds = "test_duration_seconds"
        case retries
        case repetitions
        case persistence
        case collectMetrics = "collect_metrics"
        case testSuiteName = "test_suite_name"
        case resumeFromTestNumber = "resume_from_test_number"
    }
}

public struct ServerEntry: Decodable, Sendable {
    public let hostname: String
    public let ip: String
}

public struct AnalyticsSettings: Decodable, Sendable {
    public let baseUrl: String?
    public let apiKey: String?
    public let machineName: String?
}

public enum TestPersistence: String, Decodable, Sendable {
    case file
    case inMemory
}

public struct ScenarioAxes: Decodable, Sendable {
    let compactionThresholds: [Int]?
    let peerCounts: [Int]?
    let operationCounts: [Int]?
    let concurrencyLevels: [Int]?
    let cpuLimits: [String]? // optional axis (legacy)
    let memoryLimits: [String]? // optional axis (legacy)
    let instanceSizes: [InstanceSize]? // preferred combined axis
    let persistences: [TestPersistence]?
    let persistence: TestPersistence? // single-value variant

    enum CodingKeys: String, CodingKey {
        case compactionThresholds = "compaction_thresholds"
        case peerCounts = "peer_counts"
        case operationCounts = "operation_counts"
        case concurrencyLevels = "concurrency_levels"
        case cpuLimits = "cpu_limits"
        case memoryLimits = "memory_limits"
        case instanceSizes = "instance_sizes"
        case persistences
        case persistence
    }
}

public struct InstanceSize: Decodable, Sendable {
    let cpuLimits: String?
    let memoryLimits: String?

    enum CodingKeys: String, CodingKey {
        case cpuLimits = "cpu_limits"
        case memoryLimits = "memory_limits"
    }
}

public struct TestCombination: Sendable {
    public let image: String
    public let compactionThreshold: Int
    public let peers: Int
    public let operations: Int
    public let concurrency: Int
    public let cpuLimit: String?
    public let memoryLimit: String?
    public let persistence: TestPersistence
    public let scenarioName: String?
    public let useDistributedActorSystem: Bool
    public let useManualLocks: Bool
    public let testDurationSeconds: Int?

    var description: String {
        "{image: \(image), compaction: \(compactionThreshold), peers: \(peers), ops: \(operations), conc: \(concurrency), cpu: \(cpuLimit ?? "nil"), mem: \(memoryLimit ?? "nil"), persistence: \(persistence), scenario: \(scenarioName ?? "nil"), DAS: \(useDistributedActorSystem), manualLocks: \(useManualLocks), duration: \(testDurationSeconds.map(String.init) ?? "nil")}"
    }
}

public struct RepetitionResult: Sendable {
    public let repetition: Int
    public let status: Status

    public enum Status: String, Sendable {
        case success
        case failed
        case timeout
        case interrupted
    }
}

public struct TestResult: Sendable {
    public let testNumber: Int
    public let parameters: TestCombination
    public let success: Bool
    public let duration: TimeInterval
    public let repetitions: [RepetitionResult]
}
