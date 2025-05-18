import ArgumentParser
import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import Logging

final class Peer: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "peer",
        abstract: "Start a raft peer node",
    )

    lazy var logger = Logger(label: "raft.Peer.\(id)")

    @Option(help: "The ID of this server")
    var id: UInt32

    @Option(help: "The port to listen on for incoming connections")
    var port: UInt32 = 10001

    @Option(help: "The list of peers in the format 'id:name:port,...'.")
    var peers: [Raft_Peer]

    @Option(help: "Maximum retry attempts for connecting to peers")
    var maxRetries: Int = GlobalConfig.maxRetries

    @Option(help: "Delay between retry attempts in seconds")
    var retryDelay: Double = GlobalConfig.retryDelay

    func run() async throws {
        let node = RaftNode(Raft_Peer(id: id, address: "0.0.0.0", port: port), config: RaftConfig(), peers: peers)
        let peerService = PeerService(node: node)
        let clientService = ClientService(node: node)

        let server = GRPCServer(
            transport: .http2NIOPosix(
                address: .ipv4(host: "0.0.0.0", port: Int(port)),
                transportSecurity: .plaintext
            ),
            services: [peerService, clientService]
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await server.serve()
            }

            if let address = try await server.listeningAddress {
                logger.info("Server listening on \(address)")
            }

            logger.info("Starting node")
            await node.start()
        }
    }
}
