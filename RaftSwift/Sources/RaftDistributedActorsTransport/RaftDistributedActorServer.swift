import DistributedCluster
import Logging
import RaftCore

/// A Raft server that uses distributed actors for communication
public final class RaftDistributedActorServer: RaftNodeApplication, PeerConnectable {
    /// The ID of this server
    let id: Int
    /// The port to listen on for incoming connections
    let port: Int
    /// The list of peers
    let peers: [Peer]
    /// The logger
    let logger = Logger(label: "raft.RaftDistributedActorServer")

    public init(id: Int, port: Int, peers: [Peer]) {
        self.id = id
        self.port = port
        self.peers = peers
    }

    public func serve() async throws {
        let ownPeer = Peer(id: id, address: "0.0.0.0", port: port)

        var node: RaftNode? = nil

        let actorSystem = await ClusterSystem("raft.DistributedActorSystem.\(id)") { settings in
            settings.bindPort = ownPeer.port
            settings.bindHost = ownPeer.address
            settings.downingStrategy = .timeout(.default)
        }

        let transport = DistributedActorPeerTransport(nodeProvider: { node }, peers: peers, actorSystem: actorSystem)

        node = RaftNode(
            ownPeer,
            peers: peers,
            config: RaftConfig(),
            transport: transport
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
