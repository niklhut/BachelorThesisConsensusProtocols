import ArgumentParser
import RaftCore
import RaftDistributedActorsTransport
import RaftGRPCTransport

final class Peer: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "peer",
        abstract: "Start a raft peer node",
    )

    @Option(help: "The ID of this server")
    var id: Int

    @Option(help: "The address to listen on for incoming connections")
    var address: String = "0.0.0.0"

    @Option(help: "The port to listen on for incoming connections")
    var port: Int = 10001

    @Option(help: "The list of peers in the format 'id:name:port,...'.")
    var peers: [RaftCore.Peer]

    @Flag(help: "Use Distributed Actor System for transport")
    var useDistributedActorSystem: Bool = false

    func run() async throws {
        let ownPeer = RaftCore.Peer(id: id, address: address, port: port)
        let server: any RaftNodeApplication = if useDistributedActorSystem {
            RaftDistributedActorServer(ownPeer: ownPeer, peers: peers)
        } else {
            RaftGRPCServer(ownPeer: ownPeer, peers: peers)
        }
        try await server.serve()
    }
}
