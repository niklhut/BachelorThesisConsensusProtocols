package raft.utils.types

/**
 * Leader only volatile state tracking next log entry to send to each follower
 */
data class LeaderState(
    /**
     * For each follower, index of the next log entry to send to that follower
     */
    var nextIndex: MutableMap<Int, Int> = mutableMapOf(),
    /**
     * For each follower, index of highest log entry known to be replicated on that follower
     */
    var matchIndex: MutableMap<Int, Int> = mutableMapOf()
)
