import ArgumentParser
import RaftCore
import RaftGRPCTransport

final class Peer: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "peer",
        abstract: "Start a raft peer node",
    )

    @Option(help: "The ID of this server")
    var id: Int

    @Option(help: "The port to listen on for incoming connections")
    var port: Int = 10001

    @Option(help: "The list of peers in the format 'id:name:port,...'.")
    var peers: [RaftCore.Peer]

    @Flag(help: "Use Distributed Actor System for transport")
    var distributedActorSystem: Bool = false

    func run() async throws {
        // let server: any RaftNodeApplication = if distributedActorSystem {
        //     RaftDistributedActorSystemServer(id: id, port: port, peers: peers)
        // } else {
        //     RaftGRPCServer(id: id, port: port, peers: peers)
        // }
        let server: any RaftNodeApplication = RaftGRPCServer(id: id, port: port, peers: peers)
        try await server.serve()
    }
}
