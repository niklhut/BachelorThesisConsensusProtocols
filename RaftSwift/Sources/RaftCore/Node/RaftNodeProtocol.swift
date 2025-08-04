public protocol RaftNodeProtocol: Sendable {
    func requestVote(request: RequestVoteRequest) async -> RequestVoteResponse
    func appendEntries(request: AppendEntriesRequest) async -> AppendEntriesResponse
    func installSnapshot(request: InstallSnapshotRequest) async throws -> InstallSnapshotResponse

    func put(request: PutRequest) async throws -> PutResponse
    func get(request: GetRequest) async -> GetResponse
    func getDebug(request: GetRequest) async -> GetResponse
    func getState() async -> ServerStateResponse
    func getTerm() async -> ServerTermResponse
    func getDiagnostics() async -> DiagnosticsResponse

    func start() async
}
