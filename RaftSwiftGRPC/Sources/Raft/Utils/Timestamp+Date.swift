import Foundation
import SwiftProtobuf

extension Google_Protobuf_Timestamp {
    var date: Date {
        Date(timeIntervalSince1970: Double(seconds) + Double(nanos) / 1_000_000_000)
    }

    public init(_ date: Date) {
        self.init()
        let timeInterval = date.timeIntervalSince1970
        seconds = Int64(timeInterval)
        nanos = Int32((timeInterval - Double(seconds)) * 1_000_000_000)
    }

    static func now() -> Google_Protobuf_Timestamp {
        .init(Date())
    }
}
