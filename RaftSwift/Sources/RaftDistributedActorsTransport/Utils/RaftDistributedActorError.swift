import RaftCore

enum RaftDistributedActorError: Error {
    case peerNotFound(peer: Peer)
    case peerBlocked(peer: Peer)
}
