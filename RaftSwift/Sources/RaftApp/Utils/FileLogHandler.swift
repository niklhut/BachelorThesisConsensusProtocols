import Foundation
import Logging

public struct FileLogHandler: LogHandler {
    let label: String
    private let fileHandle: FileHandle
    public var logLevel: Logger.Level = .info
    public var metadata: Logger.Metadata = [:]

    public init(label: String, fileURL: URL) throws {
        self.label = label
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

    public func log(level: Logger.Level, message: Logger.Message, metadata: Logger.Metadata?, source: String, file: String, function: String, line: UInt) {
        let fullMetadata = (metadata?.isEmpty == false ? metadata! : self.metadata)
        let logLine = "[\(level)] \(label): \(message) \(fullMetadata)\n"
        if let data = logLine.data(using: .utf8) {
            fileHandle.write(data)
        }
    }

    func shutdown() {
        try? fileHandle.close()
    }
}
