package raft.core.utils.client

import raft.core.utils.types.MetricsSample
import java.time.Instant

data class DiagnosticsRequest(
    /**
     * The start time of the diagnostics request
     */
    val start: Instant,
    /**
     * The end time of the diagnostics request
     */
    val end: Instant
)

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
    val compactionThreshold: Int,
    /**
     * The metrics collected by the server
     */
    val metrics: List<MetricsSample>
)
