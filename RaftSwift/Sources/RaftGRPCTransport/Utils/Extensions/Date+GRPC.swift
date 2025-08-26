import Foundation
import SwiftProtobuf

extension Date {
    func toGRPC() -> SwiftProtobuf.Google_Protobuf_Timestamp {
        let seconds = Int64(timeIntervalSince1970)
        let nanoseconds = Int64((timeIntervalSince1970 - Double(seconds)) * 1_000_000_000)

        var timestamp = SwiftProtobuf.Google_Protobuf_Timestamp()
        timestamp.seconds = seconds
        timestamp.nanos = Int32(nanoseconds)
        return timestamp
    }

    static func fromGRPC(_ timestamp: SwiftProtobuf.Google_Protobuf_Timestamp) -> Date {
        Date(timeIntervalSince1970: TimeInterval(timestamp.seconds) + TimeInterval(timestamp.nanos) / 1_000_000_000)
    }
}
