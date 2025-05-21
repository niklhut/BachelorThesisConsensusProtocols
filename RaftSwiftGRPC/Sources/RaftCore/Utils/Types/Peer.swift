/// A peer in the cluster
public struct Peer: Sendable, Codable, Hashable, Identifiable {
    /// The ID of the peer
    public let id: Int

    /// The address of the peer
    public let address: String

    /// The port of the peer
    public let port: Int

    /// Creates a new peer
    /// - Parameters:
    ///   - id: The ID of the peer
    ///   - address: The address of the peer
    ///   - port: The port of the peer
    public init(id: Int, address: String, port: Int) {
        self.id = id
        self.address = address
        self.port = port
    }
}
