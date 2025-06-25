package raft.core.utils.types

/**
 * Volatile state maintained in memory
 */
data class VolatileState(
    /**
     * Index of highest log entry known to be committed
     */
    var commitIndex: Int = 0,
    /**
     * Index of highest log entry applied to state machine
     */
    var lastApplied: Int = 0,
    /**
     * Current role/state of this server
     */
    var state: ServerState = ServerState.FOLLOWER,
    /**
     * ID of the current leader, if this node is a follower
     */
    var currentLeaderID: Int? = null,
    /**
     * Last heartbeat time
     */
    var lastHeartbeat: Long = System.currentTimeMillis(),
    /**
     * Election timeout
     */
    var electionTimeout: Int = 0
)