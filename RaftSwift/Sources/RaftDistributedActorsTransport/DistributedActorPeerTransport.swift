import Distributed
import DistributedCluster
import Foundation
import RaftCore

extension DistributedReception.Key {
    static var raftNode: DistributedReception.Key<DistributedActorNodeTransport> {
        "raftNode"
    }
}

/// A peer transport that uses distributed actors for communication
distributed actor DistributedActorNodeTransport: RaftNodeTransport, LifecycleWatch, PeerDiscovery {
    typealias ActorSystem = ClusterSystem

    let peers: [Peer]
    var blockedPeerIds: Set<Peer.ID> = []
    var remoteActors: [Peer: DistributedActorNodeTransport] = [:]
    var listingTask: Task<Void, Never>?

    /// The node provider
    private let nodeProvider: () -> (any RaftNodeProtocol)?

    /// The node
    var node: (any RaftNodeProtocol)!

    /// Initializes the peer transport
    /// - Parameters:
    ///   - nodeProvider: The node provider
    ///   - peers: The list of peers
    ///   - actorSystem: The actor system
    init(nodeProvider: @escaping () -> (any RaftNodeProtocol)?, peers: [Peer], actorSystem: ActorSystem) {
        self.nodeProvider = nodeProvider
        self.peers = peers
        self.actorSystem = actorSystem
    }

    /// Sets the node using the node provider passed during initialization
    distributed func setNode() {
        guard let node = nodeProvider() else {
            actorSystem.log.warning("Node not found")
            return
        }

        self.node = node
    }

    // MARK: - Peer Discovery

    distributed func getRemoteActor(_ peer: Peer) throws -> DistributedActorNodeTransport {
        guard !blockedPeerIds.contains(peer.id) else {
            actorSystem.log.info("Peer \(peer) is blocked")
            throw RaftDistributedActorError.peerBlocked(peer: peer)
        }

        guard let remoteActor = remoteActors[peer] else {
            throw RaftDistributedActorError.peerNotFound(peer: peer)
        }

        return remoteActor
    }

    // MARK: - RaftNodeTransport

    distributed func appendEntries(
        _ request: AppendEntriesRequest,
        to peer: Peer,
    ) async throws -> AppendEntriesResponse {
        let remoteActor = try getRemoteActor(peer)

        return try await remoteActor.getAppendEntries(request)
    }

    /// Handles append entries requests
    /// - Parameter request: The append entries request
    /// - Returns: The append entries response
    distributed func getAppendEntries(
        _ request: AppendEntriesRequest,
    ) async -> AppendEntriesResponse {
        await node.appendEntries(request: request)
    }

    distributed func requestVote(
        _ request: RequestVoteRequest,
        to peer: Peer,
    ) async throws -> RequestVoteResponse {
        let remoteActor = try getRemoteActor(peer)

        return try await remoteActor.getRequestVote(request)
    }

    /// Handles request vote requests
    /// - Parameter request: The request vote request
    /// - Returns: The request vote response
    distributed func getRequestVote(
        _ request: RequestVoteRequest,
    ) async -> RequestVoteResponse {
        await node.requestVote(request: request)
    }

    /// Handles install snapshot requests
    /// - Parameter request: The install snapshot request
    /// - Returns: The install snapshot response
    distributed func installSnapshot(
        _ request: InstallSnapshotRequest,
        on peer: Peer,
    ) async throws -> InstallSnapshotResponse {
        let remoteActor = try getRemoteActor(peer)

        return try await remoteActor.getInstallSnapshot(request)
    }

    /// Handles install snapshot requests
    /// - Parameter request: The install snapshot request
    /// - Returns: The install snapshot response
    distributed func getInstallSnapshot(
        _ request: InstallSnapshotRequest,
    ) async throws -> InstallSnapshotResponse {
        try await node.installSnapshot(request: request)
    }
}
