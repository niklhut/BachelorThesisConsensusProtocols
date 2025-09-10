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

    public func createService(_ args: String, background: Bool = true) throws {
        // docker service create ... (non-blocking)
        let cmd = "docker service create --with-registry-auth \(args)"
        logger.debug("Executing command: \(cmd)")
        if background {
            // spawn and return immediately
            let task = Process()
            task.executableURL = URL(fileURLWithPath: "/bin/bash")
            task.arguments = ["-lc", cmd]
            try task.run()
            // don't wait
        } else {
            let (code, _, err) = try Shell.run(cmd)
            if code != 0 {
                throw NSError(domain: "Docker", code: Int(code), userInfo: [NSLocalizedDescriptionKey: String(describing: err)])
            }
        }
    }
}
