package raft.core.utils.types

import java.time.Instant

/**
 * Represents a sample of system metrics.
 */
data class MetricsSample(
    /** The timestamp of the sample */
    val timestamp: Instant,
    /** The CPU usage percentage */
    val cpu: Double,
    /** The memory usage in MB */
    val memoryMB: Double
)