enum RaftGRPCError: Error {
    case invalidServerState
    case transportError(String)
}
