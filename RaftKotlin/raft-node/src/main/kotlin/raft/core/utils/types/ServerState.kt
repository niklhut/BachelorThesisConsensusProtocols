package raft.core.utils.types

/**
 * The current role of the server in the Raft cluster
 */
enum class ServerState {
    /**
     * Passive role; waits for messages from leader
     */
    FOLLOWER,
    /**
     * Candidate trying to gather votes to become leader
     */
    CANDIDATE,
    /**
     * Active leader role that sends heartbeats and manages log replication
     */
    LEADER
}
