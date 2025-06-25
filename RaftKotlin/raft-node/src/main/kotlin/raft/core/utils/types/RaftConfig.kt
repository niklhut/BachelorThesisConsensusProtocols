package raft.core.utils.types

/**
 * Configuration for the Raft node.
 */
data class RaftConfig(
    /**
     * The range of election timeout in milliseconds
     */
    val electionTimeoutRange: IntRange = 300 .. 600,
    /**
     * The interval of heartbeats in milliseconds
     */
    val heartbeatInterval: Int = 50,
    /**
     * The threshold of log entries to compact
     */
    val compactionThreshold: Int = 1000
)