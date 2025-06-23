package raft.node

import raft.types.Peer

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

    constructor(ownPeer: Peer, peers: MutableList<Peer>, persistence: RaftNodePersistence) {
        this.ownPeer = ownPeer
        this.peers = peers
        this.persistence = persistence
    }

    abstract fun serve()
}