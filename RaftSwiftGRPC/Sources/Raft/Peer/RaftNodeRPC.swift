import GRPCCore

protocol RaftNodeRPC: Sendable {
    /// Handle the "AppendEntries" method.
    ///
    /// - Parameters:
    ///   - request: A `Raft_AppendEntriesRequest` message.
    ///   - context: Context providing information about the RPC.
    /// - Throws: Any error which occurred during the processing of the request. Thrown errors
    ///     of type `RPCError` are mapped to appropriate statuses. All other errors are converted
    ///     to an internal error.
    /// - Returns: A `Raft_AppendEntriesResponse` to respond with.
    func appendEntries(
        request: Raft_AppendEntriesRequest,
        context: GRPCCore.ServerContext
    ) async throws -> Raft_AppendEntriesResponse

    /// Handle the "RequestVote" method.
    ///
    /// - Parameters:
    ///   - request: A `Raft_RequestVoteRequest` message.
    ///   - context: Context providing information about the RPC.
    /// - Throws: Any error which occurred during the processing of the request. Thrown errors
    ///     of type `RPCError` are mapped to appropriate statuses. All other errors are converted
    ///     to an internal error.
    /// - Returns: A `Raft_RequestVoteResponse` to respond with.
    func requestVote(
        request: Raft_RequestVoteRequest,
        context: GRPCCore.ServerContext
    ) async throws -> Raft_RequestVoteResponse

    /// Handle the "InstallSnapshot" method.
    ///
    /// - Parameters:
    ///   - request: A `Raft_InstallSnapshotRequest` message.
    ///   - context: Context providing information about the RPC.
    /// - Throws: Any error which occurred during the processing of the request. Thrown errors
    ///     of type `RPCError` are mapped to appropriate statuses. All other errors are converted
    ///     to an internal error.
    /// - Returns: A `Raft_InstallSnapshotResponse` to respond with.
    func installSnapshot(
        request: Raft_InstallSnapshotRequest,
        context: GRPCCore.ServerContext
    ) async throws -> Raft_InstallSnapshotResponse
}
