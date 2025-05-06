import ArgumentParser
@_exported import CollectionConcurrencyKit
import DistributedCluster

typealias DefaultDistributedActorSystem = ClusterSystem

@main
struct Raft: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "raft",
        abstract: "Raft distributed system",
        subcommands: [Peer.self, Client.self]
    )
}
