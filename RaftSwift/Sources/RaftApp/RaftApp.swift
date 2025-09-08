import ArgumentParser
import Dispatch

@main
struct Raft: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "raft",
        abstract: "Raft distributed system",
        subcommands: [
            Peer.self,
            Client.self,
            TestManager.self,
        ],
    )
}
