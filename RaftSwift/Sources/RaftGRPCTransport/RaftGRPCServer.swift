import GRPCCore
import GRPCNIOTransportHTTP2
import Logging
import RaftCore

/// A Raft server that uses gRPC for communication
public final class RaftGRPCServer: RaftNodeApplication {
    /// The ID of this server
    let id: Int
    /// The port to listen on for incoming connections
    let port: Int
    /// The list of peers
    let peers: [Peer]
    /// The logger
    let logger = Logger(label: "raft.RaftGRPCServer")

    public init(id: Int, port: Int, peers: [Peer]) {
        self.id = id
        self.port = port
        self.peers = peers
    }

    public func serve() async throws {
        let ownPeer = Peer(id: id, address: "0.0.0.0", port: port)
        let node = RaftNode(
            ownPeer,
            peers: peers,
            config: RaftConfig(),
            transport: GRPCPeerTransport(clientPool: GRPCClientPool(interceptors: [
                ServerIDInjectionInterceptor(peerID: id),
            ]))
        )
        let peerService = PeerService(node: node)
        let clientService = ClientService(node: node)

        let partitionInterceptor = NetworkPartitionInterceptor(logger: logger)
        let partitionService = PartitionService(partitionController: partitionInterceptor)

        let server = GRPCServer(
            transport: .http2NIOPosix(
                address: .ipv4(host: ownPeer.address, port: ownPeer.port),
                transportSecurity: .plaintext
            ),
            services: [
                peerService,
                clientService,
                partitionService,
            ],
            interceptors: [
                partitionInterceptor,
            ]
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
