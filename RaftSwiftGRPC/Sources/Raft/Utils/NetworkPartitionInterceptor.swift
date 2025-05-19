import GRPCCore
import Logging

actor NetworkPartitionInterceptor: ServerInterceptor {
    private var blockedPeers: Set<UInt32>
    private let logger: Logger

    init(logger: Logger) {
        blockedPeers = []
        self.logger = logger
    }

    func blockPeers(_ peers: [UInt32]) {
        blockedPeers.formUnion(peers)
        logger.info("Blocked peers: \(blockedPeers)")
    }

    func clearBlockedPeers() {
        blockedPeers = []
        logger.info("Cleared blocked peers")
    }

    func intercept<Input, Output>(
        request: StreamingServerRequest<Input>,
        context: ServerContext,
        next: @Sendable (
            StreamingServerRequest<Input>,
            ServerContext
        ) async throws -> StreamingServerResponse<Output>
    ) async throws -> StreamingServerResponse<Output>
        where Input: Sendable, Output: Sendable
    {
        if let peerId = request.metadata[stringValues: "x-peer-id"].first(where: { _ in true }) {
            if blockedPeers.contains(UInt32(peerId)!) {
                throw RPCError(code: .unavailable, message: "Request blocked by network partition")
            }
        }

        return try await next(request, context)
    }
}
