import Foundation
import RaftCore
import SwiftProtobuf

extension MetricsSample {
    func toGRPC() -> Raft_MetricsSample {
        .with { entry in
            entry.timestamp = Google_Protobuf_Timestamp(date: timestamp)
            entry.cpu = cpu
            entry.memoryMb = memoryMB
        }
    }

    static func fromGRPC(_ sample: Raft_MetricsSample) -> MetricsSample {
        MetricsSample(
            timestamp: sample.timestamp.date,
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
