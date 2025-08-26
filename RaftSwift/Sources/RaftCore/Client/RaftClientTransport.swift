/// Abstraction over transport layer for Raft clients.
/// This allows for different transport layers to be used, e.g. GRPC or Swift Distributed Actors.
public protocol RaftClientTransport: Sendable {
    // MARK: - Helpers

    /// Resets the clients in the stored client pool.
    func resetClients() async throws

    // MARK: - RPC Calls

    /// Sends a Get request to the specified peer.
    /// The get request only succeeds if the node is a leader.
    /// - Parameters:
    ///   - request: The Get request to send.
    ///   - peer: The peer to send the request to.
    ///   - isolation: The isolation to use for the request.
    /// - Returns: The response from the peer.
    /// - Throws: An error if the request could not be sent.
    func get(
        _ request: GetRequest,
        from peer: Peer,
        isolation: isolated (any Actor),
    ) async throws -> GetResponse

    /// Sends a GetDebug request to the specified peer.
    /// The getDebug request always succeeds.
    /// - Parameters:
    ///   - request: The GetDebug request to send.
    ///   - peer: The peer to send the request to.
    ///   - isolation: The isolation to use for the request.
    /// - Returns: The response from the peer.
    /// - Throws: An error if the request could not be sent.
    func getDebug(
        _ request: GetRequest,
        from peer: Peer,
        isolation: isolated (any Actor),
    ) async throws -> GetResponse

    /// Sends a Put request to the specified peer.
    /// The put request only succeeds if the node is a leader.
    /// - Parameters:
    ///   - request: The Put request to send.
    ///   - peer: The peer to send the request to.
    ///   - isolation: The isolation to use for the request.
    /// - Returns: The response from the peer.
    /// - Throws: An error if the request could not be sent.
    func put(
        _ request: PutRequest,
        to peer: Peer,
        isolation: isolated (any Actor),
    ) async throws -> PutResponse

    /// Sends a GetServerState request to the specified peer.
    /// - Parameters:
    ///   - peer: The peer to send the request to.
    ///   - isolation: The isolation to use for the request.
    /// - Returns: The response from the peer.
    /// - Throws: An error if the request could not be sent.
    func getServerState(
        of peer: Peer,
        isolation: isolated (any Actor),
    ) async throws -> ServerStateResponse

    /// Sends a GetTerm request to the specified peer.
    /// - Parameters:
    ///   - peer: The peer to send the request to.
    ///   - isolation: The isolation to use for the request.
    /// - Returns: The response from the peer.
    /// - Throws: An error if the request could not be sent.
    func getTerm(
        of peer: Peer,
        isolation: isolated (any Actor),
    ) async throws -> ServerTermResponse

    /// Sends a GetDiagnostics request to the specified peer.
    /// - Parameters:
    ///   - peer: The peer to send the request to.
    ///   - isolation: The isolation to use for the request.
    /// - Returns: The response from the peer.
    /// - Throws: An error if the request could not be sent.
    func getDiagnostics(
        _ request: DiagnosticsRequest,
        of peer: Peer,
        isolation: isolated (any Actor),
    ) async throws -> DiagnosticsResponse
}
