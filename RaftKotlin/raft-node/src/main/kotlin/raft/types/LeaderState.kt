package raft.types

/**
 * Leader only volatile state tracking next log entry to send to each follower
 */
data class LeaderState(
    /**
     * For each follower, index of the next log entry to send to that follower
     */
    val nextIndex: MutableMap<Int, Int> = mutableMapOf(),
    /**
     * For each follower, index of highest log entry known to be replicated on that follower
     */
    val matchIndex: MutableMap<Int, Int> = mutableMapOf()
)
