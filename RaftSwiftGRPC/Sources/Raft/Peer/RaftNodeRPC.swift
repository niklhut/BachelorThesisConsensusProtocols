import GRPCCore

protocol RaftNodeRPC: Sendable, Actor {
    /// The persistent state of the node.
    var persistentState: Raft_PersistentState { get }
    /// The volatile state of the node.
    var volatileState: Raft_VolatileState { get }
    /// The leader state of the node.
    var leaderState: Raft_LeaderState { get }

    // MARK: - Server RPCs

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

    // MARK: - Client RPCs

    /// Handle the "Put" method.
    ///
    /// > Source IDL Documentation:
    /// >
    /// > Add or update a key-value pair
    ///
    /// - Parameters:
    ///   - request: A `Raft_PutRequest` message.
    ///   - context: Context providing information about the RPC.
    /// - Throws: Any error which occurred during the processing of the request. Thrown errors
    ///     of type `RPCError` are mapped to appropriate statuses. All other errors are converted
    ///     to an internal error.
    /// - Returns: A `Raft_PutResponse` to respond with.
    func put(
        request: Raft_PutRequest,
        context: GRPCCore.ServerContext
    ) async throws -> Raft_PutResponse

    /// Handle the "Get" method.
    ///
    /// > Source IDL Documentation:
    /// >
    /// > Retrieve a value by key
    ///
    /// - Parameters:
    ///   - request: A `Raft_GetRequest` message.
    ///   - context: Context providing information about the RPC.
    /// - Throws: Any error which occurred during the processing of the request. Thrown errors
    ///     of type `RPCError` are mapped to appropriate statuses. All other errors are converted
    ///     to an internal error.
    /// - Returns: A `Raft_GetResponse` to respond with.
    func get(
        request: Raft_GetRequest,
        context: GRPCCore.ServerContext
    ) async throws -> Raft_GetResponse

    /// Handle the "GetDebug" method.
    ///
    /// > Source IDL Documentation:
    /// >
    /// > Retrieve a value by key, also returns when not a leader
    ///
    /// - Parameters:
    ///   - request: A `Raft_GetRequest` message.
    ///   - context: Context providing information about the RPC.
    /// - Throws: Any error which occurred during the processing of the request. Thrown errors
    ///     of type `RPCError` are mapped to appropriate statuses. All other errors are converted
    ///     to an internal error.
    /// - Returns: A `Raft_GetResponse` to respond with.
    func getDebug(
        request: Raft_GetRequest,
        context: GRPCCore.ServerContext
    ) async throws -> Raft_GetResponse

    // MARK: - Admin RPCs

    /// Handle the "GetServerState" method.
    ///
    /// > Source IDL Documentation:
    /// >
    /// > Get the state of a server
    ///
    /// - Throws: Any error which occurred during the processing of the request. Thrown errors
    ///     of type `RPCError` are mapped to appropriate statuses. All other errors are converted
    ///     to an internal error.
    /// - Returns: A `Raft_ServerStateResponse` to respond with.
    func getState() async throws -> Raft_ServerStateResponse

    /// Handle the "GetServerTerm" method.
    ///
    /// > Source IDL Documentation:
    /// >
    /// > Get the term of a server
    ///
    /// - Throws: Any error which occurred during the processing of the request. Thrown errors
    ///     of type `RPCError` are mapped to appropriate statuses. All other errors are converted
    ///     to an internal error.
    /// - Returns: A `Raft_ServerTermResponse` to respond with.
    func getTerm() async throws -> Raft_ServerTermResponse
}
