import Distributed
import RaftCore
import RaftTest

extension DistributedActorClientTransport: RaftPartitionTransport {
    func blockPeers(
        _ request: RaftTest.BlockPeerRequest,
        at peer: RaftCore.Peer,
        isolation: isolated any Actor
    ) async throws {
        let remoteActor = try await getRemoteActor(peer)

        try await remoteActor.blockPeers(request)
    }

    func clearBlockedPeers(
        at peer: RaftCore.Peer,
        isolation: isolated any Actor
    ) async throws {
        let remoteActor = try await getRemoteActor(peer)

        try await remoteActor.clearBlockedPeers()
    }
}

extension DistributedActorPeerTransport {
    distributed func blockPeers(
        _ request: RaftTest.BlockPeerRequest,
    ) async throws {
        blockedPeerIds.formUnion(request.peerIds)
    }

    distributed func clearBlockedPeers() async throws {
        blockedPeerIds = []
    }
}
