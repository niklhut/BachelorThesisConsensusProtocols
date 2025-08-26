package raft.transport.grpc.utils.extensions

import com.google.protobuf.Timestamp
import raft.Types
import raft.core.utils.types.MetricsSample
import raft.metricsSample

/** Extension function to convert a LogEntry to its gRPC representation. */
fun MetricsSample.toGRPC(): Types.MetricsSample {
    return metricsSample {
        timestamp = Timestamp.newBuilder()
            .setSeconds(this@toGRPC.timestamp.epochSecond)
            .setNanos(this@toGRPC.timestamp.nano)
            .build()
        cpu = this@toGRPC.cpu
        memoryMB = this@toGRPC.memoryMB
    }
}
fun List<MetricsSample>.toGRPC(): List<Types.MetricsSample> {
    return map { it.toGRPC() }
}
