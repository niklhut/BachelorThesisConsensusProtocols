import Distributed
import DistributedCluster
import RaftCore

distributed actor DistributedActorClientTransport: RaftClientTransport, LifecycleWatch, PeerDiscovery {
    typealias ActorSystem = ClusterSystem

    let peers: [Peer]
    var remoteActors: [Peer: DistributedActorNodeTransport] = [:]
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

    distributed func resetClients() async throws {
        remoteActors = [:]
    }

    func get(
        _ request: GetRequest,
        from peer: Peer,
        isolation: isolated any Actor,
    ) async throws -> GetResponse {
        let remoteActor = try await getRemoteActor(peer)

        return try await remoteActor.get(request)
    }

    func getDebug(
        _ request: GetRequest,
        from peer: Peer,
        isolation: isolated any Actor,
    ) async throws -> GetResponse {
        let remoteActor = try await getRemoteActor(peer)

        return try await remoteActor.getDebug(request)
    }

    func put(
        _ request: PutRequest,
        to peer: Peer,
        isolation: isolated any Actor,
    ) async throws -> PutResponse {
        let remoteActor = try await getRemoteActor(peer)

        return try await remoteActor.put(request)
    }

    func getServerState(
        of peer: Peer,
        isolation: isolated any Actor,
    ) async throws -> ServerStateResponse {
        let remoteActor = try await getRemoteActor(peer)

        return try await remoteActor.getServerState()
    }

    func getTerm(
        of peer: Peer,
        isolation: isolated any Actor,
    ) async throws -> ServerTermResponse {
        let remoteActor = try await getRemoteActor(peer)

        return try await remoteActor.getTerm()
    }

    func getDiagnostics(
        of peer: Peer,
        isolation: isolated any Actor,
    ) async throws -> DiagnosticsResponse {
        let remoteActor = try await getRemoteActor(peer)

        return try await remoteActor.getDiagnostics()
    }
}

extension DistributedActorNodeTransport {
    distributed func get(
        _ request: GetRequest,
    ) async -> GetResponse {
        await node.get(request: request)
    }

    distributed func getDebug(
        _ request: GetRequest,
    ) async -> GetResponse {
        await node.getDebug(request: request)
    }

    distributed func put(
        _ request: PutRequest,
    ) async throws -> PutResponse {
        try await node.put(request: request)
    }

    distributed func getServerState() async -> ServerStateResponse {
        await node.getState()
    }

    distributed func getTerm() async -> ServerTermResponse {
        await node.getTerm()
    }

    distributed func getDiagnostics() async -> DiagnosticsResponse {
        let diagnostics = await node.getDiagnostics()

        return DiagnosticsResponse(
            id: diagnostics.id,
            implementation: diagnostics.implementation + " (Distributed Actors)",
            version: diagnostics.version,
            compactionThreshold: diagnostics.compactionThreshold,
        )
    }
}
