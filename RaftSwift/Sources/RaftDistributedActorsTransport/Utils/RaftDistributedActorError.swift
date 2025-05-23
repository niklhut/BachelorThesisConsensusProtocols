import RaftCore

enum RaftDistributedActorError: Error {
    case peerNotFound(peer: Peer)
}
