import Foundation
import Logging
import Rainbow

struct ColoredConsoleLogHandler: LogHandler {
    var metadata: Logging.Logger.Metadata

    let stream: StreamLogHandler
    var logLevel: Logger.Level = .info
    let label: String

    init(label: String) {
        self.label = label
        stream = StreamLogHandler.standardOutput(label: label)
        metadata = [:]
    }

    subscript(metadataKey key: String) -> Logging.Logger.Metadata.Value? {
        get {
            metadata[key]
        }
        set {
            metadata[key] = newValue
        }
    }

    func log(level: Logger.Level, message: Logger.Message, metadata: Logger.Metadata?, source: String, file: String, function: String, line: UInt) {
        var formattedMessage = "\(Date()) \(level): \(message)"

        switch level {
        case .info:
            formattedMessage = formattedMessage.applyingColor(.green)
        case .notice:
            formattedMessage = formattedMessage.applyingColor(.blue)
        case .warning:
            formattedMessage = formattedMessage.applyingColor(.yellow)
        case .error:
            formattedMessage = formattedMessage.applyingColor(.red)
        case .critical:
            formattedMessage = formattedMessage.applyingCodes(NamedColor.white, BackgroundColor.red, Style.bold)
        default:
            break
        }

        stream.log(level: level, message: .init(stringLiteral: formattedMessage), metadata: metadata, source: source, file: file, function: function, line: line)
    }
}
