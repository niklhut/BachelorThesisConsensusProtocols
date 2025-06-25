package raft.types

/**
 * Errors that can occur in the Raft node.
 */
sealed class RaftError: Throwable() {
    /**
     * Thrown when a method is called on a node that is not the leader,
     * but the method requires the node to be the leader.
     */
    object NotLeader: RaftError()
}
