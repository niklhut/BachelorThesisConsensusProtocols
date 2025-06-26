package raft.transport.grpc

import io.grpc.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Interceptor that blocks requests from certain peers for network partition simulation.
 */
class NetworkPartitionInterceptor : ServerInterceptor {

    private val blockedPeers = mutableSetOf<Int>()
    private val mutex = Mutex()
    private val logger: Logger = LoggerFactory.getLogger(NetworkPartitionInterceptor::class.java)

    /**
     * Blocks the given peers.
     * @param peers The peers to block
     */
    suspend fun blockPeers(peers: List<Int>) {
        mutex.withLock {
            blockedPeers.addAll(peers)
            logger.info("Blocked peers: $blockedPeers")
        }
    }

    /**
     * Clears all blocked peers.
     */
    suspend fun clearBlockedPeers() {
        mutex.withLock {
            blockedPeers.clear()
            logger.info("Cleared blocked peers")
        }
    }

    override fun <ReqT, RespT> interceptCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {
        val peerIdHeader = headers.get(Metadata.Key.of("x-peer-id", Metadata.ASCII_STRING_MARSHALLER))

        if (peerIdHeader != null) {
            val peerId = peerIdHeader.toIntOrNull()
            if (peerId != null && peerId in blockedPeers) {
                call.close(
                    Status.UNAVAILABLE.withDescription("Request blocked by network partition"),
                    Metadata()
                )
                return object : ServerCall.Listener<ReqT>() {}
            }
        }

        return next.startCall(call, headers)
    }
}