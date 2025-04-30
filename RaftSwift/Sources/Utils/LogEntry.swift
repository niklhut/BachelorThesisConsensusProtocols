struct LogEntryValue: Codable, Equatable {
    var key: String
    var value: String
}

struct LogEntry: Codable, Equatable {
    var term: Int
    var data: [LogEntryValue]
}

    