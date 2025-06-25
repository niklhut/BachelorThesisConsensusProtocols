package raft.core.utils.types

/**
 * A peer in the cluster
 */
data class Peer(
    /**
     * The ID of the peer
     */
    val id: Int,
    /**
     * The address of the peer
     */
    val address: String,
    /**
     * The port of the peer
     */
    val port: Int
)
