import RaftCore

extension Peer {
    func toGRPC() -> Raft_Peer {
        .with { peer in
            peer.id = UInt32(id)
            peer.address = address
            peer.port = UInt32(port)
        }
    }

    static func fromGRPC(_ peer: Raft_Peer) throws -> Peer {
        Peer(
            id: Int(peer.id),
            address: peer.address,
            port: Int(peer.port)
        )
    }
}
