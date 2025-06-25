package raft.utils.types

/**
 * A single entry in the replicated log
 */
data class LogEntry(
    /**
     * The term in which the log entry was created
     */
    val term: Int,
    /**
     * The key of the log entry, optional for no-op entries
     */
    val key: String? = null,
    /**
     * The value of the log entry, null if this is a deletion or no-op
     */
    val value: String? = null
)
