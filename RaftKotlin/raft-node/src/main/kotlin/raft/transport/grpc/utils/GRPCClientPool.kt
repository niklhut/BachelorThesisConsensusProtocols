package raft.transport.grpc.utils

import io.grpc.ClientInterceptor
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import raft.core.utils.types.Peer
import java.net.InetAddress

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
        val address = InetAddress.getByName(peer.address)
        return ManagedChannelBuilder
            .forAddress(address.hostAddress, peer.port)
            .usePlaintext()
            .intercept(interceptors)
            .build()
    }

    fun reset() {
        clients.values.forEach { it.shutdownNow() }
        clients.clear()
    }
}
