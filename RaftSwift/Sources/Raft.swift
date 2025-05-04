@_exported import CollectionConcurrencyKit
import ArgumentParser

@main
struct Raft: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "raft",
        abstract: "Raft distributed system",
        subcommands: [Peer.self, Client.self]
    )
}
