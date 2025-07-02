package raft.core.utils.client

/**
 * A response to a implementation version request.
 */
data class DiagnosticsResponse(
    /**
     * The ID of the server
     */
    val id: Int,
    /**
     * The implementation version of the server
     */
    val implementation: String,
    /**
     * The version of the Raft implementation
     */
    val version: String,
    /**
     * The compaction threshold of the server
     */
    val compactionThreshold: Int
)
