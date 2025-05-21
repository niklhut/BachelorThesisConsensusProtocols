import Distributed
import DistributedCluster
import Foundation
import RaftCore

extension DistributedReception.Key {
    static var raftNode: DistributedReception.Key<DistributedActorPeerTransport> {
        "raftNode"
    }
}

distributed actor DistributedActorPeerTransport: RaftPeerTransport, LifecycleWatch {
    typealias ActorSystem = ClusterSystem

    let peers: [Peer]
    var remoteActors: [Peer: DistributedActorPeerTransport] = [:]
    private let nodeProvider: () -> RaftNode?

    weak var node: RaftNode!

    var listingTask: Task<Void, Never>?

    init(nodeProvider: @escaping () -> RaftNode?, peers: [Peer], actorSystem: ActorSystem) {
        self.nodeProvider = nodeProvider
        self.peers = peers
        self.actorSystem = actorSystem
    }

    distributed func getRemoteActor(_ peer: Peer) throws -> DistributedActorPeerTransport {
        guard let remoteActor = remoteActors[peer] else {
            throw RaftDistributedActorError.peerNotFound
        }

        return remoteActor
    }

    distributed func setNode() {
        guard let node = nodeProvider() else {
            actorSystem.log.warning("Node not found")
            return
        }

        self.node = node
    }

    func appendEntries(
        _ request: AppendEntriesRequest,
        to peer: Peer,
        isolation: isolated any Actor
    ) async throws -> AppendEntriesResponse {
        let remoteActor = try await getRemoteActor(peer)

        return try await remoteActor.getAppendEntries(request)
    }

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

                print("New Actor with host: \(host), port: \(port)")

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
