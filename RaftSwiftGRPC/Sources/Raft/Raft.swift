import ArgumentParser
@_exported import CollectionConcurrencyKit

@main
struct Raft: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "raft",
        abstract: "Raft distributed system",
    )
}
