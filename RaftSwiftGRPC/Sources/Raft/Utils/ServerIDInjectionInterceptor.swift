import GRPCCore

struct ServerIDInjectionInterceptor: ClientInterceptor {
    let peerID: UInt32

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
