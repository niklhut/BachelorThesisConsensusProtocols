package raft.transport.grpc.utils.extensions

import raft.Types
import raft.core.utils.types.Snapshot
import raft.snapshot

fun Snapshot.toGRPC(): Types.Snapshot {
    return snapshot {
        lastIncludedIndex = this@toGRPC.lastIncludedIndex.toLong()
        lastIncludedTerm = this@toGRPC.lastIncludedTerm.toLong()
        stateMachine.putAll(this@toGRPC.stateMachine)
    }
}