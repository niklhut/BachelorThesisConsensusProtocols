import GRPCCore

/// Injects the peer ID into the request metadata
struct ServerIDInjectionInterceptor: ClientInterceptor {
    /// The ID of the peer sending the requests
    let peerID: Int

    func intercept<Input, Output>(
        request: StreamingClientRequest<Input>,
        context: ClientContext,
        next: (
            _ request: StreamingClientRequest<Input>,
            _ context: ClientContext
        ) async throws -> StreamingClientResponse<Output>
    ) async throws -> StreamingClientResponse<Output>
        where Input: Sendable, Output: Sendable
    {
        var request = request
        request.metadata.addString(String(peerID), forKey: "x-peer-id")
        return try await next(request, context)
    }
}
