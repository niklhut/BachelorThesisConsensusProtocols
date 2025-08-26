package raft.core.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.isActive
import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.coroutineContext
import java.time.Instant
import raft.core.utils.types.MetricsSample

class ContainerException(message: String) : Exception(message)

class MetricsCollector(
    private val interval: kotlin.time.Duration,
    maxSamples: Int
) {

    private val buffer: Array<MetricsSample?> = arrayOfNulls(maxSamples)
    private var index = 0
    private var lastCPUUsage: Long = 0
    private var lastTimestamp: Instant = Instant.now()
    private var collectionJob: Job? = null
    private val isStopped = AtomicBoolean(false)

    init {
        if (!isRunningInContainer()) {
            throw ContainerException("Not running in a container")
        }
        // Start the collection coroutine
        collectionJob = CoroutineScope(Dispatchers.IO).launch {
            startCollection()
        }
    }

    private fun isRunningInContainer(): Boolean {
        if (File("/.dockerenv").exists()) return true
        val cgroup = File("/proc/1/cgroup")
        if (cgroup.exists()) {
            val content = cgroup.readText()
            return content.contains("docker") || content.contains("kubepods")
        }
        return false
    }

    private suspend fun startCollection() {
        lastCPUUsage = readCPUUsage()
        lastTimestamp = Instant.now()

        // Use coroutineContext.isActive to check cancellation inside a suspend function.
        while (coroutineContext.isActive && !isStopped.get()) {
            delay(interval)

            val now = Instant.now()
            val currentCPUUsage = readCPUUsage()
            val memory = readMemory()

            val cpuPercentage = if (lastCPUUsage > 0) {
                val cpuDelta = currentCPUUsage - lastCPUUsage // microseconds
                val timeDeltaMicros =
                    java.time.Duration.between(lastTimestamp, now).toMillis() * 1_000 // microseconds
                (cpuDelta.toDouble() / timeDeltaMicros) * 100.0
            } else {
                0.0
            }

            buffer[index] = MetricsSample(
                timestamp = now,
                cpu = cpuPercentage,
                memoryMB = memory.toDouble() / (1024.0 * 1024.0)
            )

            lastCPUUsage = currentCPUUsage
            lastTimestamp = now
            index = (index + 1) % buffer.size
        }
    }

    private fun readCPUUsage(): Long {
        val cpuStat = File("/sys/fs/cgroup/cpu.stat")
        if (!cpuStat.exists()) return 0
        val usageLine = cpuStat.readLines().firstOrNull { it.startsWith("usage_usec") } ?: return 0
        val parts = usageLine.split(" ")
        return parts.getOrNull(1)?.toLongOrNull() ?: 0
    }

    private fun readMemory(): Long {
        val memFile = File("/sys/fs/cgroup/memory.current")
        if (!memFile.exists()) return 0
        return memFile.readText().trim().toLongOrNull() ?: 0
    }

    fun getSamples(start: Instant, end: Instant): List<MetricsSample> {
        return buffer.filterNotNull().filter {
            it.timestamp.isAfter(start) && it.timestamp.isBefore(end)
        }
    }

    fun stop() {
        isStopped.set(true)
        collectionJob?.cancel()
        collectionJob = null
    }
}
