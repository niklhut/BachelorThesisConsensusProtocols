import Foundation
import Logging

/// A lightweight helper to run shell commands and manage Docker Swarm services.
public enum Shell {
    @discardableResult
    static func run(_ command: String, timeout: TimeInterval? = nil) throws -> (code: Int32, out: String, err: String) {
        let task = Process()
        task.executableURL = URL(fileURLWithPath: "/bin/bash")
        task.arguments = ["-lc", command]

        let stdout = Pipe()
        task.standardOutput = stdout
        let stderr = Pipe()
        task.standardError = stderr

        try task.run()

        if let timeout {
            let deadline = Date().addingTimeInterval(timeout)
            while task.isRunning, Date() < deadline {
                Thread.sleep(forTimeInterval: 0.05)
            }
            if task.isRunning {
                task.terminate()
            }
        }

        task.waitUntilExit()
        let outData = stdout.fileHandleForReading.readDataToEndOfFile()
        let errData = stderr.fileHandleForReading.readDataToEndOfFile()
        return (task.terminationStatus, String(data: outData, encoding: .utf8) ?? "", String(data: errData, encoding: .utf8) ?? "")
    }
}

public struct DockerSwarmServiceManager {
    let servicePrefix: String
    let logger: Logger

    public func removeAllServices() {
        let listCmd = "docker service ls --format '{{.Name}}'"
        logger.debug("Executing command: \(listCmd)")
        guard let (_, out, _) = try? Shell.run(listCmd) else { return }
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
                throw NSError(domain: "Docker", code: Int(code), userInfo: [NSLocalizedDescriptionKey: err])
            }
        }
    }

    public func serviceLogs(_ name: String) -> String {
        let command = "docker service logs \(name)"
        logger.debug("Executing command: \(command)")
        let (_, out, _) = try! Shell.run(command, timeout: 10)
        return out
    }
}
