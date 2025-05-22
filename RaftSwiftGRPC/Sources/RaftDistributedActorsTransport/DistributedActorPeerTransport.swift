import Distributed
import DistributedCluster
import Foundation
import RaftCore

extension DistributedReception.Key {
    static var raftNode: DistributedReception.Key<DistributedActorPeerTransport> {
        "raftNode"
    }
}

/// A peer transport that uses distributed actors for communication
distributed actor DistributedActorPeerTransport: RaftPeerTransport, LifecycleWatch {
    typealias ActorSystem = ClusterSystem

    /// The list of peers
    let peers: [Peer]
    /// The remote actors mapped by peer
    var remoteActors: [Peer: DistributedActorPeerTransport] = [:]
    /// The node provider
    private let nodeProvider: () -> RaftNode?

    /// The node
    weak var node: RaftNode!

    /// The listing task, used to find peers
    var listingTask: Task<Void, Never>?

    /// Initializes the peer transport
    /// - Parameters:
    ///   - nodeProvider: The node provider
    ///   - peers: The list of peers
    ///   - actorSystem: The actor system
    init(nodeProvider: @escaping () -> RaftNode?, peers: [Peer], actorSystem: ActorSystem) {
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

    /// Gets the remote actor for the given peer
    /// - Parameter peer: The peer
    /// - Returns: The remote actor
    /// - Throws: RaftDistributedActorError.peerNotFound if the remote actor is not found
    private distributed func getRemoteActor(_ peer: Peer) throws -> DistributedActorPeerTransport {
        guard let remoteActor = remoteActors[peer] else {
            throw RaftDistributedActorError.peerNotFound
        }

        return remoteActor
    }

    func appendEntries(
        _ request: AppendEntriesRequest,
        to peer: Peer,
        isolation: isolated any Actor
    ) async throws -> AppendEntriesResponse {
        let remoteActor = try await getRemoteActor(peer)

        return try await remoteActor.getAppendEntries(request)
    }

    /// Handles append entries requests
    /// - Parameter request: The append entries request
    /// - Returns: The append entries response
    /// - Throws: An error if the request could not be processed
    distributed func getAppendEntries(
        _ request: AppendEntriesRequest,
    ) async throws -> AppendEntriesResponse {
        try await node.appendEntries(request: request)
    }

    func requestVote(
        _ request: RequestVoteRequest,
        to peer: Peer,
        isolation: isolated any Actor
    ) async throws -> RequestVoteResponse {
        let remoteActor = try await getRemoteActor(peer)

        return try await remoteActor.getRequestVote(request)
    }

    /// Handles request vote requests
    /// - Parameter request: The request vote request
    /// - Returns: The request vote response
    /// - Throws: An error if the request could not be processed
    distributed func getRequestVote(
        _ request: RequestVoteRequest,
    ) async throws -> RequestVoteResponse {
        try await node.requestVote(request: request)
    }

    // MARK: - LifecycleWatch

    func terminated(actor id: ActorID) async {
        if let index = remoteActors.firstIndex(where: { $0.value.id == id }) {
            remoteActors.remove(at: index)
        }
    }

    /// Finds peers using the actor system
    distributed func findPeers() {
        guard listingTask == nil else {
            actorSystem.log.warning("Already looking for peers")
            return
        }

        listingTask = Task {
            for await remoteActor in await actorSystem.receptionist.listing(of: .raftNode) {
                actorSystem.log.info("Found peer: \(remoteActor.id)")

                let address = remoteActor.id

                guard let url = URL(string: address.description),
                      let host = url.host,
                      let port = url.port
                else {
                    actorSystem.log.warning("Found peer with unknown address: \(remoteActor.id)")
                    continue
                }

                guard let remoteActorPeer = peers.first(where: { $0.address == host && $0.port == port }) else {
                    actorSystem.log.warning("Found peer with unknown address: \(remoteActor.id)")
                    continue
                }

                actorSystem.log.info("\nFound peer: \(remoteActorPeer)\n")

                remoteActors[remoteActorPeer] = remoteActor
                watchTermination(of: remoteActor)
            }
        }
    }
}
