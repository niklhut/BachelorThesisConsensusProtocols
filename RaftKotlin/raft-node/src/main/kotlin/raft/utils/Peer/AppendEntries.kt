package raft.utils.peer

import raft.utils.types.LogEntry

/**
 * Leader to Follower, used for log replication and heartbeats
 */
data class AppendEntriesRequest(
    /**
     * Leader's current term
     */
    val term: Int,
    /**
     * ID of the leader making the request
     */
    val leaderId: Int,
    /**
     * Index of log entry immediately preceding new ones
     */
    val prevLogIndex: Int,
    /**
     * Term of prevLogIndex entry
     */
    val prevLogTerm: Int,
    /**
     * Log entries to store
     */
    val entries: List<LogEntry>,
    /**
     * Leader's commit index
     */
    val leaderCommit: Int
)

/**
 * Follower to Leader, response to AppendEntriesRequest
 */
data class AppendEntriesResponse(
    /**
     * Current term, for leader to update itself
     */
    val term: Int,
    /**
     * True if follower contained entry matching prevLogIndex and prevLogTerm
     */
    val success: Boolean
)