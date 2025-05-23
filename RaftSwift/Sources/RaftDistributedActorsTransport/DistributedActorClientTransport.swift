import Distributed
import DistributedCluster
import RaftCore

distributed actor DistributedActorClientTransport: RaftClientTransport, LifecycleWatch, PeerDiscovery {
    typealias ActorSystem = ClusterSystem

    let peers: [Peer]
    var remoteActors: [Peer: DistributedActorPeerTransport] = [:]
    var listingTask: Task<Void, Never>?

    /// Initializes the client transport
    /// - Parameters:
    ///   - peers: The list of peers
    ///   - actorSystem: The actor system
    init(peers: [Peer], actorSystem: ActorSystem) {
        self.peers = peers
        self.actorSystem = actorSystem
    }

    // MARK: - RaftClientTransport

    func get(
        _ request: GetRequest,
        from peer: Peer,
        isolation: isolated any Actor
    ) async throws -> GetResponse {
        let remoteActor = try await getRemoteActor(peer)

        return try await remoteActor.get(request)
    }

    func getDebug(
        _ request: GetRequest,
        from peer: Peer,
        isolation: isolated any Actor
    ) async throws -> GetResponse {
        let remoteActor = try await getRemoteActor(peer)

        return try await remoteActor.getDebug(request)
    }

    func put(
        _ request: PutRequest,
        to peer: Peer,
        isolation: isolated any Actor
    ) async throws -> PutResponse {
        let remoteActor = try await getRemoteActor(peer)

        return try await remoteActor.put(request)
    }

    func getServerState(
        of peer: Peer,
        isolation: isolated any Actor
    ) async throws -> ServerStateResponse {
        let remoteActor = try await getRemoteActor(peer)

        return try await remoteActor.getServerState()
    }

    func getTerm(
        of peer: Peer,
        isolation: isolated any Actor
    ) async throws -> ServerTermResponse {
        let remoteActor = try await getRemoteActor(peer)

        return try await remoteActor.getTerm()
    }
}

extension DistributedActorPeerTransport {
    distributed func get(
        _ request: GetRequest,
    ) async throws -> GetResponse {
        try await node.get(request: request)
    }

    distributed func getDebug(
        _ request: GetRequest,
    ) async throws -> GetResponse {
        try await node.getDebug(request: request)
    }

    distributed func put(
        _ request: PutRequest,
    ) async throws -> PutResponse {
        try await node.put(request: request)
    }

    distributed func getServerState() async throws -> ServerStateResponse {
        try await node.getState()
    }

    distributed func getTerm() async throws -> ServerTermResponse {
        try await node.getTerm()
    }
}
