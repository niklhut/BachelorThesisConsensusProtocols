package raft.transport.grpc.utils

import io.grpc.ClientInterceptor
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.util.concurrent.ConcurrentHashMap
import raft.core.utils.types.Peer

class GRPCClientPool(
    private val interceptors: List<ClientInterceptor> = emptyList()
) {
    private val clients: MutableMap<Int, ManagedChannel> = ConcurrentHashMap()

    suspend fun clientFor(peer: Peer): ManagedChannel {
        return clients.getOrPut(peer.id) {
            createChannel(peer)
        }
    }

    private fun createChannel(peer: Peer): ManagedChannel {
        return ManagedChannelBuilder
            .forAddress(peer.address, peer.port)
            .usePlaintext()
            .intercept(interceptors)
            .build()
    }

    fun reset() {
        clients.values.forEach { it.shutdownNow() }
        clients.clear()
    }
}
