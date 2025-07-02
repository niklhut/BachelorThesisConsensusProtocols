package raft.transport.grpc

import io.grpc.netty.NettyServerBuilder
import io.grpc.Server
import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import raft.core.node.RaftNode
import raft.core.node.RaftNodeApplication
import raft.core.node.RaftNodePersistence
import raft.core.utils.types.Peer
import raft.core.utils.types.RaftConfig
import raft.transport.grpc.services.ClientService
import raft.transport.grpc.services.PartitionService
import raft.transport.grpc.services.PeerService
import raft.transport.grpc.utils.GRPCClientPool
import java.net.InetAddress
import java.net.InetSocketAddress

/**
 * A Raft server that uses gRPC for communication.
 */
class RaftGRPCServer(
    ownPeer: Peer,
    peers: MutableList<Peer>,
    persistence: RaftNodePersistence
) : RaftNodeApplication(ownPeer, peers, persistence) {

    private val logger: Logger = LoggerFactory.getLogger(RaftGRPCServer::class.java)
    private var server: Server? = null

    override fun serve() {
        runBlocking {
            serveAsync()
        }
    }

    /**
     * Starts the server asynchronously.
     */
    suspend fun serveAsync() {
        val node = RaftNode(
            ownPeer = ownPeer,
            peers = peers,
            config = RaftConfig(),
            transport = GRPCNodeTransport(
                clientPool = GRPCClientPool(
                    interceptors = listOf(
                        ServerIDInjectionInterceptor(peerID = ownPeer.id)
                    )
                )
            ),
            persistence = persistence
        )

        val peerService = PeerService(node)
        val clientService = ClientService(node)
        val partitionInterceptor = NetworkPartitionInterceptor()
        val partitionService = PartitionService(partitionInterceptor)

        server = NettyServerBuilder.forAddress(
            InetSocketAddress(InetAddress.getByName(ownPeer.address), ownPeer.port)
        )
            .addService(peerService)
            .addService(clientService)
            .addService(partitionService)
            .intercept(partitionInterceptor)
            .build()

        server?.start()
        logger.info("Server listening on ${ownPeer.address}:${ownPeer.port}")

        // Start the Raft node (won't be blocked by awaitTermination)
        val nodeJob = CoroutineScope(Dispatchers.Default).launch {
            logger.info("Starting node")
            node.start()
        }

        // Wait for the server to terminate (in blocking context)
        withContext(Dispatchers.IO) {
            server?.awaitTermination()
        }

        // Optionally, wait for node job to complete or cancel it when server shuts down
        nodeJob.cancelAndJoin()
    }

    /**
     * Shuts down the server gracefully.
     */
    suspend fun shutdown() {
        withContext(Dispatchers.IO) {
            server?.shutdown()
            server?.awaitTermination()
        }
        logger.info("Server shut down")
    }
}