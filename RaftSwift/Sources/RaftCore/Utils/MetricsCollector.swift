import Foundation

enum ContainerError: Error {
    case notInContainer
}

/// An actor that periodically collects lightweight container-level metrics (CPU and memory)
/// and stores them in a fixed-size circular buffer for later retrieval.
public actor MetricsCollector: Sendable {
    private let interval: Duration
    private var buffer: [MetricsSample?]
    private var index = 0
    private var lastCPUUsage: UInt64 = 0
    private var lastTimestamp: Date = .init()
    private var timerTask: Task<Void, Never>?

    /// Initializes a new MetricsCollector.
    /// - Parameters:
    ///   - interval: The sampling interval.
    ///   - maxSamples: The maximum number of samples to retain.
    /// - Throws: An error if the collector cannot be initialized.
    init(interval: Duration, maxSamples: Int) throws {
        guard MetricsCollector.isRunningInContainer() else {
            throw ContainerError.notInContainer
        }
        self.interval = interval
        buffer = Array(repeating: nil, count: maxSamples)
        timerTask = nil

        Task {
            await self.startCollectionTask()
        }
    }

    /// Starts the background task for collecting metrics.
    private func startCollectionTask() {
        timerTask = Task {
            await self.startCollection()
        }
    }

    deinit {
        timerTask?.cancel()
    }

    /// Checks if the process is running inside a container.
    /// - Returns: A Boolean value indicating whether the process is in a container.
    private static func isRunningInContainer() -> Bool {
        if FileManager.default.fileExists(atPath: "/.dockerenv") { return true }
        if let content = try? String(contentsOfFile: "/proc/1/cgroup", encoding: .utf8) {
            return content.contains("docker") || content.contains("kubepods")
        }
        return false
    }

    /// The main collection loop for gathering metrics.
    private func startCollection() async {
        lastCPUUsage = readCPUUsage()
        lastTimestamp = Date()

        while !Task.isCancelled {
            try? await Task.sleep(for: interval)

            let now = Date()
            let currentCPUUsage = readCPUUsage()
            let memory = readMemory()

            let cpuPercentage: Double
            if lastCPUUsage > 0 {
                let cpuDelta = currentCPUUsage - lastCPUUsage // in microseconds
                let timeDelta = now.timeIntervalSince(lastTimestamp) * 1_000_000 // convert to microseconds

                // CPU percentage = (cpu_time_used / wall_time_elapsed) * 100
                cpuPercentage = Double(cpuDelta) / timeDelta * 100
            } else {
                cpuPercentage = 0.0
            }

            buffer[index] = MetricsSample(
                timestamp: now,
                cpu: cpuPercentage,
                memoryMB: Double(memory) / (1024.0 * 1024.0),
            )

            // Update for next iteration
            lastCPUUsage = currentCPUUsage
            lastTimestamp = now
            index = (index + 1) % buffer.count
        }
    }

    /// Reads the current CPU usage from the cgroup filesystem.
    /// - Returns: The CPU usage in microseconds, or 0 if it cannot be read.
    private func readCPUUsage() -> UInt64 {
        guard let data = try? String(contentsOfFile: "/sys/fs/cgroup/cpu.stat", encoding: .utf8) else {
            return 0
        }

        guard let usageLine = data.split(separator: "\n").first(where: { $0.hasPrefix("usage_usec") }) else {
            return 0
        }

        let components = usageLine.split(separator: " ")
        guard components.count >= 2, let usage = UInt64(components[1]) else {
            return 0
        }

        return usage // Keep in microseconds
    }

    /// Reads the current memory usage from the cgroup filesystem.
    /// - Returns: The memory usage in bytes, or 0 if it cannot be read.
    private func readMemory() -> UInt64 {
        guard let data = try? String(contentsOfFile: "/sys/fs/cgroup/memory.current", encoding: .utf8) else {
            return 0
        }

        return UInt64(data.trimmingCharacters(in: .whitespacesAndNewlines)) ?? 0
    }

    /// Retrieves the collected metrics samples within a specific time range.
    /// - Parameters:
    ///   - start: The start date of the time range.
    ///   - end: The end date of the time range.
    /// - Returns: An array of metrics samples collected within the specified time range.
    func getSamples(start: Date, end: Date) -> [MetricsSample] {
        buffer.compactMap(\.self).filter { $0.timestamp > start && $0.timestamp < end }
    }

    /// Stops the metrics collection.
    func stop() {
        timerTask?.cancel()
        timerTask = nil
    }
}
