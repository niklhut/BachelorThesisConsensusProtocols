package raft.transport.grpc.utils.extensions

import raft.Types
import raft.core.utils.types.Peer
import raft.peer

fun Peer.toGRPC(): Types.Peer {
    return peer {
        id = this@toGRPC.id
        address = this@toGRPC.address
        port = this@toGRPC.port
    }
}