import DistributedCluster
import Foundation
import RaftCore

protocol PeerDiscovery where Self: LifecycleWatch {
    /// The list of peers
    var peers: [Peer] { get }
    /// The remote actors mapped by peer
    var remoteActors: [Peer: DistributedActorNodeTransport] { get set }
    /// The listing task used to find peers
    var listingTask: Task<Void, Never>? { get set }

    /// Finds peers using the actor system
    distributed func findPeers()

    /// Gets the remote actor for the given peer
    /// - Parameter peer: The peer
    /// - Returns: The remote actor
    /// - Throws: RaftDistributedActorError.peerNotFound if the remote actor is not found
    distributed func getRemoteActor(_ peer: Peer) throws -> DistributedActorNodeTransport
}

extension PeerDiscovery {
    distributed func findPeers() {
        guard listingTask == nil else {
            actorSystem.log.warning("Already looking for peers")
            return
        }

        listingTask = Task {
            for await remoteActor in await actorSystem.receptionist.listing(of: .raftNode) {
                actorSystem.log.info("Found peer: \(remoteActor.id)")

                let address = remoteActor.id

                guard let url = URL(string: address.description),
                      let host = url.host,
                      let port = url.port
                else {
                    actorSystem.log.warning("Found peer with unknown address: \(remoteActor.id)")
                    continue
                }

                guard let remoteActorPeer = peers.first(where: { $0.address == host && $0.port == port }) else {
                    actorSystem.log.warning("Found peer with unknown address: \(remoteActor.id)")
                    continue
                }

                actorSystem.log.info("Found peer: \(remoteActorPeer)")

                remoteActors[remoteActorPeer] = remoteActor
                watchTermination(of: remoteActor)
            }
        }
    }

    distributed func getRemoteActor(_ peer: Peer) throws -> DistributedActorNodeTransport {
        guard let remoteActor = remoteActors[peer] else {
            throw RaftDistributedActorError.peerNotFound(peer: peer)
        }

        return remoteActor
    }

    func terminated(actor id: ActorID) async {
        if let index = remoteActors.firstIndex(where: { $0.value.id == id }) {
            remoteActors.remove(at: index)
        }
    }
}
