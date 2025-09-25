import GRPCCore
import GRPCNIOTransportHTTP2
import Logging
import RaftCore

/// A Raft server that uses gRPC for communication
public final class RaftGRPCServer: RaftNodeApplication {
    public let ownPeer: Peer
    public let peers: [Peer]
    public let persistence: any RaftNodePersistence
    public let collectMetrics: Bool
    public let useManualLock: Bool

    /// The logger
    let logger = Logger(label: "raft.RaftGRPCServer")

    public init(
        ownPeer: Peer,
        peers: [Peer],
        persistence: any RaftNodePersistence,
        collectMetrics: Bool,
        useManualLock: Bool
    ) {
        self.ownPeer = ownPeer
        self.peers = peers
        self.persistence = persistence
        self.collectMetrics = collectMetrics
        self.useManualLock = useManualLock
    }

    public func serve() async throws {
        let node: any RaftNodeProtocol = if useManualLock {
            RaftNodeManualLock(
                ownPeer,
                peers: peers,
                config: RaftConfig(),
                transport: GRPCNodeTransport(clientPool: GRPCClientPool(interceptors: [
                    ServerIDInjectionInterceptor(peerID: ownPeer.id),
                ])),
                persistence: persistence,
                collectMetrics: collectMetrics,
            )
        } else {
            RaftNode(
                ownPeer,
                peers: peers,
                config: RaftConfig(),
                transport: GRPCNodeTransport(clientPool: GRPCClientPool(interceptors: [
                    ServerIDInjectionInterceptor(peerID: ownPeer.id),
                ])),
                persistence: persistence,
                collectMetrics: collectMetrics,
            )
        }
        let peerService = PeerService(node: node)
        let clientService = ClientService(node: node)

        let partitionInterceptor = NetworkPartitionInterceptor(logger: logger)
        let partitionService = PartitionService(partitionController: partitionInterceptor)

        let server: GRPCServer<HTTP2ServerTransport.Posix> = GRPCServer(
            transport: .http2NIOPosix(
                address: .ipv4(host: "0.0.0.0", port: ownPeer.port),
                transportSecurity: .plaintext,
                config: HTTP2ServerTransport.Posix.Config.defaults(configure: { config in
                    config.rpc.maxRequestPayloadSize = 100 * 1024 * 1024 // 100 MB
                }),
            ),
            services: [
                peerService,
                clientService,
                partitionService,
            ],
            interceptors: [
                partitionInterceptor,
            ],
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
