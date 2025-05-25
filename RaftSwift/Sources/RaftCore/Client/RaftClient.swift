import Foundation
import Logging

public actor RaftClient<Transport: RaftClientTransport> {
    // MARK: - Properties

    /// The transport layer to use for communication with the server.
    @_spi(TransportAccess) public let transport: Transport

    /// The logger to use for logging.
    let logger: Logger

    /// The list of peers in the cluster.
    public nonisolated let peers: [Peer]

    // MARK: - Init

    /// Initializes a new instance of the RaftClient class.
    /// - Parameters:
    ///   - peers: The list of peers in the cluster.
    ///   - transport: The transport layer to use for communication with the server.
    public init(peers: [Peer], transport: Transport) {
        self.peers = peers
        self.transport = transport
        logger = Logger(label: "raft.RaftClient")
    }

    // MARK: - RPC Calls

    /// Sends a Put request to the specified peer.
    /// The put request only succeeds if the node is a leader.
    /// - Parameters:
    ///   - request: The Put request to send.
    ///   - peer: The peer to send the request to. If nil, the request will be sent to the leader.
    ///   - isolation: The isolation to use for the request.
    /// - Returns: The response from the peer.
    /// - Throws: An error if the request could not be sent.
    public func put(request: PutRequest, to peer: Peer? = nil) async throws -> PutResponse {
        let peer = if let peer {
            peer
        } else {
            try await findLeader()
        }

        return try await transport.put(request, to: peer, isolation: #isolation)
    }

    /// Sends a Get request to the specified peer.
    /// The get request only succeeds if the node is a leader.
    /// - Parameters:
    ///   - request: The Get request to send.
    ///   - peer: The peer to send the request to. If nil, the request will be sent to the leader.
    ///   - isolation: The isolation to use for the request.
    /// - Returns: The response from the peer.
    /// - Throws: An error if the request could not be sent.
    public func get(request: GetRequest, from peer: Peer? = nil) async throws -> GetResponse {
        let peer = if let peer {
            peer
        } else {
            try await findLeader()
        }

        return try await transport.get(request, from: peer, isolation: #isolation)
    }

    /// Sends a GetDebug request to the specified peer.
    /// - Parameters:
    ///   - request: The GetDebug request to send.
    ///   - peer: The peer to send the request to.
    ///   - isolation: The isolation to use for the request.
    /// - Returns: The response from the peer.
    /// - Throws: An error if the request could not be sent.
    public func getDebug(request: GetRequest, from peer: Peer) async throws -> GetResponse {
        try await transport.getDebug(request, from: peer, isolation: #isolation)
    }

    /// Sends a GetServerState request to the specified peer.
    /// - Parameters:
    ///   - peer: The peer to send the request to.
    ///   - isolation: The isolation to use for the request.
    /// - Returns: The response from the peer.
    /// - Throws: An error if the request could not be sent.
    public func getServerState(of peer: Peer) async throws -> ServerStateResponse {
        try await transport.getServerState(of: peer, isolation: #isolation)
    }

    /// Sends a GetTerm request to the specified peer.
    /// - Parameters:
    ///   - peer: The peer to send the request to.
    ///   - isolation: The isolation to use for the request.
    /// - Returns: The response from the peer.
    /// - Throws: An error if the request could not be sent.
    public func getServerTerm(of peer: Peer) async throws -> ServerTermResponse {
        try await transport.getTerm(of: peer, isolation: #isolation)
    }

    // MARK: - Helpers

    /// Finds the leader node.
    ///
    /// - Parameter excludingPeer: The peer to exclude from the search.
    /// - Throws: An error if no leader is found.
    /// - Returns: The leader node.
    public func findLeader(excludingPeer peerToExclude: Peer? = nil) async throws -> Peer {
        var leader: Peer?

        for peer in peers where peerToExclude != peer {
            let response = try await transport.getServerState(
                of: peer,
                isolation: #isolation
            )
            if response.state == .leader {
                leader = peer
                break
            }
        }

        guard let leader else {
            throw RaftClientError.noLeaderAvailable
        }

        return leader
    }
}
