import GRPCCore
import Logging

/// Interceptor that blocks requests from certain peers
actor NetworkPartitionInterceptor: ServerInterceptor {
    /// The IDs of the peers that are blocked
    private var blockedPeers: Set<Int>

    /// The logger
    private let logger: Logger

    /// Creates a new interceptor
    /// - Parameter logger: The logger
    init(logger: Logger) {
        blockedPeers = []
        self.logger = logger
    }

    /// Blocks the given peers
    /// - Parameter peers: The peers to block
    func blockPeers(_ peers: [Int]) {
        blockedPeers.formUnion(peers)
        logger.info("Blocked peers: \(blockedPeers)")
    }

    /// Clears the blocked peers
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
            if blockedPeers.contains(Int(peerId)!) {
                throw RPCError(code: .unavailable, message: "Request blocked by network partition")
            }
        }

        return try await next(request, context)
    }
}
