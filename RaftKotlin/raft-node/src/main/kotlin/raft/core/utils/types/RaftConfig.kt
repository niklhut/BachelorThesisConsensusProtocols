package raft.core.utils.types

/**
 * Configuration for the Raft node.
 */
data class RaftConfig(
    /**
     * The range of election timeout in milliseconds
     */
    val electionTimeoutRange: IntRange = 500 .. 1000,
    /**
     * The interval of heartbeats in milliseconds
     */
    val heartbeatInterval: Int = 50,
)