import GRPCCore
import GRPCNIOTransportHTTP2
import Logging
@testable import Raft
import Testing

@Suite("Basic Raft Tests")
final class BasicRaftTests {
    var servers: [GRPCServer<HTTP2ServerTransport.Posix>] = []
    var nodes: [RaftNode] = []

    let logger = Logger(label: "raft.BasicRaftTests")

    // MARK: - Lifecycle

    init() async throws {
        // Use randomized base port to allow parallel test execution
        let basePort = Int.random(in: 10000 ... 20000)

        // First create the peers
        var peers: [Raft_Peer] = []
        for i in 1 ... 5 {
            peers.append(Raft_Peer(id: UInt32(i), address: "0.0.0.0", port: UInt32(basePort + i)))
        }

        // Then create the nodes and servers
        for peer in peers {
            let node = RaftNode(peer, config: RaftConfig(), peers: peers.filter { $0.id != peer.id })
            nodes.append(node)
            let peerService = PeerService(node: node)

            let server = GRPCServer(
                transport: .http2NIOPosix(
                    address: .ipv4(host: peer.address, port: Int(peer.port)),
                    transportSecurity: .plaintext
                ),
                services: [peerService]
            )
            servers.append(server)
        }

        try await withThrowingTaskGroup { group in
            for (server, node) in zip(servers, nodes) {
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

    deinit {
        // Shutdown all servers
        for server in servers {
            server.beginGracefulShutdown()
        }
        servers = []
        nodes = []
    }

    // MARK: - Helpers
}
