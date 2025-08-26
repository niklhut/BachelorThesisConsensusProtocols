import Foundation

enum ContainerError: Error {
    case notInContainer
}

public actor MetricsCollector: Sendable {
    private let interval: Duration
    private var buffer: [MetricsSample?]
    private var index = 0
    private var lastCPUUsage: UInt64 = 0
    private var lastTimestamp: Date = .init()
    private var timerTask: Task<Void, Never>?

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

    private func startCollectionTask() {
        timerTask = Task {
            await self.startCollection()
        }
    }

    deinit {
        timerTask?.cancel()
    }

    private static func isRunningInContainer() -> Bool {
        if FileManager.default.fileExists(atPath: "/.dockerenv") { return true }
        if let content = try? String(contentsOfFile: "/proc/1/cgroup", encoding: .utf8) {
            return content.contains("docker") || content.contains("kubepods")
        }
        return false
    }

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

    private func readMemory() -> UInt64 {
        guard let data = try? String(contentsOfFile: "/sys/fs/cgroup/memory.current", encoding: .utf8) else {
            return 0
        }

        return UInt64(data.trimmingCharacters(in: .whitespacesAndNewlines)) ?? 0
    }

    func getSamples(start: Date, end: Date) -> [MetricsSample] {
        buffer.compactMap(\.self).filter { $0.timestamp > start && $0.timestamp < end }
    }

    func stop() {
        timerTask?.cancel()
        timerTask = nil
    }
}
