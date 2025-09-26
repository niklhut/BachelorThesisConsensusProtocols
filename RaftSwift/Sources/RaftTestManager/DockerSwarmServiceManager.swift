import Foundation
import Logging

/// A lightweight helper to run shell commands and manage Docker Swarm services.
public enum Shell {
    @discardableResult
    static func run(
        _ command: String,
        timeout: TimeInterval? = nil,
        captureOutput: Bool = false,
    ) throws -> (code: Int32, out: String?, err: String?) {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/bin/bash")
        process.arguments = ["-lc", command]

        // Set up pipes only if caller requests captured output
        let stdoutPipe = captureOutput ? Pipe() : nil
        let stderrPipe = captureOutput ? Pipe() : nil
        process.standardOutput = stdoutPipe ?? FileHandle.nullDevice
        process.standardError = stderrPipe ?? FileHandle.nullDevice

        try process.run()

        // Optional timeout
        if let timeout {
            let deadline = Date().addingTimeInterval(timeout)
            while process.isRunning, Date() < deadline {
                if Task.isCancelled {
                    process.terminate()
                    return (code: -1, out: nil, err: "Process cancelled")
                }
                Thread.sleep(forTimeInterval: 0.05)
            }
            if process.isRunning {
                process.terminate()
            }
        }

        process.waitUntilExit()

        // Only read if caller explicitly wants it
        let out = captureOutput
            ? String(data: stdoutPipe!.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8)
            : nil
        let err = captureOutput
            ? String(data: stderrPipe!.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8)
            : nil

        return (process.terminationStatus, out, err)
    }
}

public struct DockerSwarmServiceManager {
    let servicePrefix: String
    let logger: Logger

    public func removeAllServices() {
        let listCmd = "docker service ls --format '{{.Name}}'"
        logger.debug("Executing command: \(listCmd)")
        guard let (_, out, _) = try? Shell.run(listCmd, captureOutput: true), let out else { return }
        for name in out.split(separator: "\n") {
            if name.hasPrefix("\(servicePrefix)raft_node") {
                let command = "docker service rm \(name)"
                logger.debug("Executing command: \(command)")
                _ = try? Shell.run(command)
            }
        }
    }

    public func createServices(_ argsList: [String], timeout: TimeInterval) async throws -> Bool {
        try await withThrowingTaskGroup { group in
            for args in argsList {
                let cmd = "docker service create --with-registry-auth \(args)"
                group.addTask { [logger] in
                    logger.debug("Executing command: \(cmd)")
                    let (code, _, err) = try Shell.run(cmd, timeout: timeout)
                    if code != 0 {
                        logger.error("Error creating service: \(String(describing: err))")
                        return false
                    }
                    return true
                }
            }

            var allSucceeded = true
            for try await success in group {
                if !success {
                    allSucceeded = false
                    group.cancelAll()
                }
            }
            return allSucceeded
        }
    }

    public func createServicesWithRetry(
        _ argsList: [String],
        timeout: TimeInterval,
        attempts: Int = 5,
    ) async -> Bool {
        for attempt in 1 ... attempts {
            logger.info("Creating services (attempt \(attempt) of \(attempts))")

            do {
                let success = try await createServices(argsList, timeout: timeout)
                if success {
                    logger.info("Services created successfully on attempt \(attempt)")
                    return true
                }
            } catch {
                logger.error("Error creating services on attempt \(attempt): \(error)")
            }

            // Only retry if there are attempts left
            if attempt < attempts {
                logger.warning("Removing all services before retrying…")
                removeAllServices()
            }
        }

        logger.error("Failed to create services after \(attempts) attempts")
        return false
    }
}
