import DistributedCluster
import Logging
import RaftCore

/// A Raft server that uses distributed actors for communication
public final class RaftDistributedActorServer: RaftNodeApplication, PeerConnectable {
    public let ownPeer: Peer
    public let peers: [Peer]
    public let persistence: any RaftNodePersistence

    /// The logger
    let logger = Logger(label: "raft.RaftDistributedActorServer")

    public init(ownPeer: Peer, peers: [Peer], persistence: any RaftNodePersistence) {
        self.ownPeer = ownPeer
        self.peers = peers
        self.persistence = persistence
    }

    public func serve() async throws {
        var node: RaftNode? = nil

        let actorSystem = await ClusterSystem("raft.DistributedActorSystem.\(ownPeer.id)") { settings in
            settings.bindPort = ownPeer.port
            settings.bindHost = ownPeer.address
            settings.downingStrategy = .timeout(.default)
        }

        let transport = DistributedActorPeerTransport(nodeProvider: { node }, peers: peers, actorSystem: actorSystem)

        node = RaftNode(
            ownPeer,
            peers: peers,
            config: RaftConfig(),
            transport: transport,
            persistence: persistence,
        )

        try await transport.setNode()

        connectToPeers(actorSystem: actorSystem)

        await node!.start()
        try await transport.findPeers()
        await actorSystem.receptionist.checkIn(transport, with: .raftNode)

        // Keep the application running
        try await actorSystem.terminated
    }
}
