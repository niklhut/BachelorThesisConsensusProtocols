import DistributedCluster

enum RaftError: Error {
    case notLeader(leaderId: ClusterSystem.ActorID?)
}
