import Foundation
import Logging

public struct FileLogHandler: LogHandler {
    let label: String
    private let fileHandle: FileHandle
    public var logLevel: Logger.Level = .info
    public var metadata: Logger.Metadata = [:]

    private static let timestampFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone.current
        return formatter
    }()

    public init(label: String, fileURL: URL) throws {
        self.label = label

        // Ensure directory exists
        let directoryURL = fileURL.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: directoryURL, withIntermediateDirectories: true)

        // Create file if it doesn't exist
        if !FileManager.default.fileExists(atPath: fileURL.path) {
            FileManager.default.createFile(atPath: fileURL.path, contents: nil)
        }

        fileHandle = try FileHandle(forWritingTo: fileURL)
        fileHandle.seekToEndOfFile()
    }

    public subscript(metadataKey key: String) -> Logger.Metadata.Value? {
        get { metadata[key] }
        set { metadata[key] = newValue }
    }

    public func log(
        level: Logger.Level,
        message: Logger.Message,
        metadata: Logger.Metadata?,
        source: String,
        file: String,
        function: String,
        line: UInt,
    ) {
        let fullMetadata = (metadata?.isEmpty == false ? metadata! : self.metadata)
        let timestamp = Self.timestampFormatter.string(from: Date())
        let logLine = "\(timestamp) [\(level)] \(label): \(message) \(fullMetadata)\n"
        if let data = logLine.data(using: .utf8) {
            fileHandle.write(data)
        }
    }

    func shutdown() {
        try? fileHandle.close()
    }
}
