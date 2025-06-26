package raft.app

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.choice
import com.github.ajalt.clikt.parameters.types.int
import kotlinx.coroutines.runBlocking
import raft.core.node.RaftNodeApplication
import raft.core.node.persistence.FileRaftNodePersistence
import raft.core.node.persistence.InMemoryRaftNodePersistence
import raft.core.utils.types.Peer
import raft.transport.grpc.RaftGRPCServer

/**
 * CLI command to start a Raft peer node.
 */
class PeerCommand : CliktCommand(
    name = "peer",
) {

    private val id by option("--id", help = "The ID of this server")
        .int()
        .required()

    private val address by option("--address", help = "The address to listen on for incoming connections")
        .default("0.0.0.0")

    private val port by option("--port", help = "The port to listen on for incoming connections")
        .int()
        .default(10001)

    private val peersString by option("--peers", help = "The list of peers in the format 'id:address:port,...'")
        .required()

    private val persistence by option("--persistence", help = "The persistence layer")
        .choice("file", "inMemory")
        .default("inMemory")

    private val compactionThreshold by option("--compaction-threshold", help = "The compaction threshold")
        .int()
        .default(1000)

    override fun run() = runBlocking {
        val ownPeer = Peer(id = id, address = address, port = port)

        val peers = parsePeers(peersString)

        val persistenceLayer = when (persistence) {
            "inMemory" -> InMemoryRaftNodePersistence(compactionThreshold = compactionThreshold)
            "file" -> FileRaftNodePersistence(compactionThreshold = compactionThreshold)
            else -> throw IllegalArgumentException("Unknown persistence type: $persistence")
        }

        val server: RaftNodeApplication =
            RaftGRPCServer(ownPeer = ownPeer, peers = peers.toMutableList(), persistence = persistenceLayer)

        server.serve()
    }

    /**
     * Parses the peers string into a list of Peer objects.
     * Format: "id:address:port,id:address:port,..."
     */
    private fun parsePeers(peersString: String): List<Peer> {
        return peersString.split(",").map { peerString ->
            val parts = peerString.trim().split(":")
            if (parts.size != 3) {
                throw IllegalArgumentException("Invalid peer format: $peerString. Expected format: id:address:port")
            }

            val peerId = parts[0].toIntOrNull()
                ?: throw IllegalArgumentException("Invalid peer ID: ${parts[0]}")
            val peerAddress = parts[1]
            val peerPort = parts[2].toIntOrNull()
                ?: throw IllegalArgumentException("Invalid peer port: ${parts[2]}")

            Peer(id = peerId, address = peerAddress, port = peerPort)
        }
    }
}
