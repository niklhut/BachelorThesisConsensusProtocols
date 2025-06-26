package raft.transport.grpc.utils.extensions

import raft.Types
import raft.core.utils.types.LogEntry
import raft.logEntry

/** Extension function to convert a LogEntry to its gRPC representation. */
fun LogEntry.toGRPC(): Types.LogEntry {
    return logEntry {
        term = this@toGRPC.term.toLong()
        val newKey = this@toGRPC.key
        if (newKey != null) {
            key = newKey
        }
        val newValue = this@toGRPC.value
        if (newValue != null) {
            value = newValue
        }
    }
}

/** Extension function to convert a List of LogEntry to a List of gRPC LogEntry. */
fun List<LogEntry>.toGRPC(): List<Types.LogEntry> {
    return map { it.toGRPC() }
}
