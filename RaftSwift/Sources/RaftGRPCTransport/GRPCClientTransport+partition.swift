import RaftCore
import RaftTest

extension GRPCClientTransport: RaftPartitionTransport {
    func blockPeers(
        _ request: RaftTest.BlockPeerRequest,
        at peer: RaftCore.Peer,
        isolation: isolated any Actor
    ) async throws {
        let client = try await clientPool.client(for: peer)
        let peerClient = Raft_Partition.Client(wrapping: client)

        let _ = try await peerClient.blockPeers(.with { grpcRequest in
            grpcRequest.peerIds = request.peerIds.map { Int32($0) }
        })
    }

    func clearBlockedPeers(
        at peer: RaftCore.Peer,
        isolation: isolated any Actor
    ) async throws {
        let client = try await clientPool.client(for: peer)
        let peerClient = Raft_Partition.Client(wrapping: client)

        let _ = try await peerClient.clearBlockedPeers(.init())
    }
}
