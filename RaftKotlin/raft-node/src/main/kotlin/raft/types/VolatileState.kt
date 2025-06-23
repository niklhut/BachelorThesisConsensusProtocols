package raft.types

/**
 * Volatile state maintained in memory
 */
data class VolatileState(
    /**
     * Index of highest log entry known to be committed
     */
    val commitIndex: Int = 0,
    /**
     * Index of highest log entry applied to state machine
     */
    val lastApplied: Int = 0,
    /**
     * Current role/state of this server
     */
    val state: ServerState = ServerState.FOLLOWER,
    /**
     * ID of the current leader, if this node is a follower
     */
    val currentLeaderID: Int? = null,
    /**
     * Last heartbeat time
     */
    val lastHeartbeat: Long = System.currentTimeMillis(),
    /**
     * Election timeout
     */
    val electionTimeout: Int = 0
)