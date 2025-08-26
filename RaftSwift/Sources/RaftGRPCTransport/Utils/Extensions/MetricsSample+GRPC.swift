import Foundation
import RaftCore

extension MetricsSample {
    func toGRPC() -> Raft_MetricsSample {
        .with { entry in
            entry.timestamp = timestamp.toGRPC()
            entry.cpu = cpu
            entry.memoryMb = memoryMB
        }
    }

    static func fromGRPC(_ sample: Raft_MetricsSample) -> MetricsSample {
        MetricsSample(
            timestamp: Date.fromGRPC(sample.timestamp),
            cpu: sample.cpu,
            memoryMB: sample.memoryMb,
        )
    }
}

extension [MetricsSample] {
    func toGRPC() -> [Raft_MetricsSample] {
        map { $0.toGRPC() }
    }

    static func fromGRPC(_ samples: [Raft_MetricsSample]) -> [MetricsSample] {
        samples.map { MetricsSample.fromGRPC($0) }
    }
}
