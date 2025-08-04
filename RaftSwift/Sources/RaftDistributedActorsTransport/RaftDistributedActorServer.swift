import DistributedCluster
import Logging
import RaftCore

/// A Raft server that uses distributed actors for communication
public final class RaftDistributedActorServer: RaftNodeApplication, PeerConnectable {
    public let ownPeer: Peer
    public let peers: [Peer]
    public let persistence: any RaftNodePersistence
    public let useManualLock: Bool

    /// The logger
    let logger = Logger(label: "raft.RaftDistributedActorServer")

    public init(ownPeer: Peer, peers: [Peer], persistence: any RaftNodePersistence, useManualLock: Bool) {
        self.ownPeer = ownPeer
        self.peers = peers
        self.persistence = persistence
        self.useManualLock = useManualLock
    }

    public func serve() async throws {
        var node: (any RaftNodeProtocol)? = nil

        let actorSystem = await ClusterSystem("raft.DistributedActorSystem.\(ownPeer.id)") { settings in
            settings.bindPort = ownPeer.port
            settings.bindHost = ownPeer.address
            settings.downingStrategy = .timeout(.default)
        }

        let transport = DistributedActorNodeTransport(nodeProvider: { node }, peers: peers, actorSystem: actorSystem)

        node = if useManualLock {
            RaftNodeManualLock(
                ownPeer,
                peers: peers,
                config: RaftConfig(),
                transport: transport,
                persistence: persistence,
            )
        } else {
            RaftNode(
                ownPeer,
                peers: peers,
                config: RaftConfig(),
                transport: transport,
                persistence: persistence,
            )
        }

        try await transport.setNode()

        connectToPeers(actorSystem: actorSystem)

        await node!.start()
        try await transport.findPeers()
        await actorSystem.receptionist.checkIn(transport, with: .raftNode)

        // Keep the application running
        try await actorSystem.terminated
    }
}
