import Foundation
import RaftCore

/// Custom errors for Raft testing
public enum RaftTestError: Error, LocalizedError {
    case noLeaderFound
    case multipleLeadersFound(count: Int)
    case leaderNotChanged(originalLeader: Peer)
    case entryReplicated(key: String, peer: Peer, expected: String?, actual: String?)
    case entryNotReplicated(key: String, peer: Peer, expected: String?, actual: String?)
    case inconsistentTerms(terms: [(Peer, Int)])
    case partitionFailed(reason: String)
    case operationFailed(operation: String, reason: String)
    case timeoutWaitingForCondition(condition: String)
    case invalidTestConfiguration(reason: String)
    case insufficientDiagnosticsResponses

    public var errorDescription: String? {
        switch self {
        case .noLeaderFound:
            "No leader found in the cluster"
        case let .multipleLeadersFound(count):
            "Multiple leaders found in the cluster: \(count)"
        case let .leaderNotChanged(originalLeader):
            "Leader did not change from original leader: \(originalLeader)"
        case let .entryReplicated(key, peer, expected, actual):
            "Entry replicated for key '\(key)' on peer \(peer): expected '\(expected ?? "nil")', got '\(actual ?? "nil")'"
        case let .entryNotReplicated(key, peer, expected, actual):
            "Entry not replicated for key '\(key)' on peer \(peer): expected '\(expected ?? "nil")', got '\(actual ?? "nil")'"
        case let .inconsistentTerms(terms):
            "Inconsistent terms across nodes: \(terms)"
        case let .partitionFailed(reason):
            "Network partition failed: \(reason)"
        case let .operationFailed(operation, reason):
            "Operation '\(operation)' failed: \(reason)"
        case let .timeoutWaitingForCondition(condition):
            "Timeout waiting for condition: \(condition)"
        case let .invalidTestConfiguration(reason):
            "Invalid test configuration: \(reason)"
        case .insufficientDiagnosticsResponses:
            "Not enough diagnostics responses received to proceed"
        }
    }
}
