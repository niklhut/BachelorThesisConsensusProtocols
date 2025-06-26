package raft.transport.grpc.utils.extensions

import raft.Types
import raft.core.utils.types.ServerState

fun ServerState.toGRPC(): Types.ServerState {
    return when (this@toGRPC) {
        ServerState.FOLLOWER -> Types.ServerState.FOLLOWER
        ServerState.CANDIDATE -> Types.ServerState.CANDIDATE
        ServerState.LEADER -> Types.ServerState.LEADER
    }
}

fun ServerState.fromGRPC(state: Types.ServerState): ServerState {
    return when (state) {
        Types.ServerState.FOLLOWER -> ServerState.FOLLOWER
        Types.ServerState.CANDIDATE -> ServerState.CANDIDATE
        Types.ServerState.LEADER -> ServerState.LEADER
        Types.ServerState.UNRECOGNIZED -> throw IllegalArgumentException("Server state cannot be recognized")
    }
}