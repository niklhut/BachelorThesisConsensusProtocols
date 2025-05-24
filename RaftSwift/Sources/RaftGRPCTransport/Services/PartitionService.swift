import GRPCCore
import RaftCore
import SwiftProtobuf

struct PartitionService: Raft_Partition.SimpleServiceProtocol {
    let partitionController: NetworkPartitionInterceptor

    init(partitionController: NetworkPartitionInterceptor) {
        self.partitionController = partitionController
    }

    func blockPeers(request: Raft_BlockPeersRequest, context: ServerContext) async throws -> Google_Protobuf_Empty {
        await partitionController.blockPeers(request.peerIds.map { Int($0) })
        return .init()
    }

    func clearBlockedPeers(request: Google_Protobuf_Empty, context: ServerContext) async throws -> Google_Protobuf_Empty {
        await partitionController.clearBlockedPeers()
        return .init()
    }
}
