/// Abstraction over transport layer for Raft peers.
/// This allows for different transport layers to be used, e.g. GRPC or Swift Distributed Actors.
public protocol RaftNodeTransport: Sendable {
    /// Sends an AppendEntries message to the specified peer.
    /// - Parameters:
    ///   - request: The AppendEntries message to send.
    ///   - peer: The peer to send the message to.
    ///   - isolation: The isolation to use for the message.
    /// - Returns: The response from the peer.
    /// - Throws: An error if the message could not be sent.
    func appendEntries(
        _ request: AppendEntriesRequest,
        to peer: Peer,
        isolation: isolated (any Actor),
    ) async throws -> AppendEntriesResponse

    /// Sends a RequestVote message to the specified peer.
    /// - Parameters:
    ///   - request: The RequestVote message to send.
    ///   - peer: The peer to send the message to.
    ///   - isolation: The isolation to use for the message.
    /// - Returns: The response from the peer.
    /// - Throws: An error if the message could not be sent.
    func requestVote(
        _ request: RequestVoteRequest,
        to peer: Peer,
        isolation: isolated (any Actor),
    ) async throws -> RequestVoteResponse

    /// Sends an InstallSnapshot message to the specified peer.
    /// - Parameters:
    ///   - request: The InstallSnapshot message to send.
    ///   - peer: The peer to send the message to.
    ///   - isolation: The isolation to use for the message.
    /// - Returns: The response from the peer.
    /// - Throws: An error if the message could not be sent.
    func installSnapshot(
        _ request: InstallSnapshotRequest,
        on peer: Peer,
        isolation: isolated (any Actor),
    ) async throws -> InstallSnapshotResponse
}
