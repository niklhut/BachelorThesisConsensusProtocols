package raft.core.node

import raft.core.utils.types.Peer

/**
 * Abstract class for a Raft node application.
 * This class provides the basic functionality for a Raft node application.
 */
abstract class RaftNodeApplication {
    /**
     * The own peer.
     */
    val ownPeer: Peer
    /**
     * The list of peers.
     */
    val peers: MutableList<Peer>
    /**
     * The persistence layer.
     */
    val persistence: RaftNodePersistence
    /**
     * Whether to collect metrics.
     */
    val collectMetrics: Boolean

    /**
     * Constructor for a Raft node application.
     *
     * @param ownPeer The own peer.
     * @param peers The list of peers.
     * @param persistence The persistence layer.
     */
    constructor(ownPeer: Peer, peers: MutableList<Peer>, persistence: RaftNodePersistence, collectMetrics: Boolean) {
        this.ownPeer = ownPeer
        this.peers = peers
        this.persistence = persistence
        this.collectMetrics = collectMetrics
    }

    /**
     * Starts the Raft node application.
     */
    abstract fun serve()
}