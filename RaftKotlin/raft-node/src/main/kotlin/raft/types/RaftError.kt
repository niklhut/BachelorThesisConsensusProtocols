package raft.types

/**
 * Errors that can occur in the Raft node.
 */
enum class RaftError {
    /**
     * Thrown when a method is called on a node that is not the leader,
     * but the method requires the node to be the leader.
     */
    NOT_LEADER
}
