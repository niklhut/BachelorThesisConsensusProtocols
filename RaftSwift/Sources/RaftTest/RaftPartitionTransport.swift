@_spi(TransportAccess) import RaftCore

/// Protocol for partitioning the network
public protocol RaftPartitionTransport: RaftClientTransport {
    /// Blocks communication between specified peer groups
    /// - Parameters:
    ///   - request: The request containing the peer IDs to block
    ///   - peer: The peer to send the request to
    ///   - isolation: The isolation to use for the request
    /// - Throws: Any errors thrown by the request
    func blockPeers(
        _ request: BlockPeerRequest,
        at peer: Peer,
        isolation: isolated (any Actor)
    ) async throws

    /// Clears the block list of the specified peer
    /// - Parameters:
    ///   - peer: The peer to send the request to
    ///   - isolation: The isolation to use for the request
    /// - Throws: Any errors thrown by the request
    func clearBlockedPeers(
        at peer: Peer,
        isolation: isolated (any Actor)
    ) async throws
}

public extension RaftClient where Transport: RaftPartitionTransport {
    /// Blocks communication between specified peer groups
    /// - Parameters:
    ///   - request: The request containing the peer IDs to block
    ///   - peer: The peer to send the request to
    /// - Throws: Any errors thrown by the request
    func blockPeers(
        _ request: BlockPeerRequest,
        at peer: Peer
    ) async throws {
        try await transport.blockPeers(request, at: peer, isolation: #isolation)
    }

    /// Clears the block list of the specified peer
    /// - Parameters:
    ///   - peer: The peer to send the request to
    /// - Throws: Any errors thrown by the request
    func clearBlockedPeers(
        at peer: Peer
    ) async throws {
        try await transport.clearBlockedPeers(at: peer, isolation: #isolation)
    }
}
