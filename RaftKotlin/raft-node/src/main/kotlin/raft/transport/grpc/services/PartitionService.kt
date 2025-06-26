package raft.transport.grpc.services

import com.google.protobuf.Empty
import raft.PartitionGrpcKt
import raft.PartitionOuterClass
import raft.transport.grpc.NetworkPartitionInterceptor

/**
 * gRPC service implementation for network partition testing.
 */
class PartitionService(
    private val partitionController: NetworkPartitionInterceptor
) : PartitionGrpcKt.PartitionCoroutineImplBase() {

    override suspend fun blockPeers(request: PartitionOuterClass.BlockPeersRequest): Empty {
        partitionController.blockPeers(request.peerIdsList.map { it.toInt() })
        return Empty.getDefaultInstance()
    }

    override suspend fun clearBlockedPeers(request: Empty): Empty {
        partitionController.clearBlockedPeers()
        return Empty.getDefaultInstance()
    }
}