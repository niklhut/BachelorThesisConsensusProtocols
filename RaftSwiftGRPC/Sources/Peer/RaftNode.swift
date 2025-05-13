import Foundation
import GRPCCore
import Logging

actor RaftNode: RaftNodeRPC {
    // MARK: - Properties

    // TODO: maybe move to volatile state
    let config: RaftConfig
    let logger: Logger
    var lastHeartbeat = Date()

    var heartbeatTask: Task<Void, Never>?

    var persistentState = Raft_PersistentState()
    var volatileState = Raft_VolatileState()
    var leaderState = Raft_LeaderState()

    init(_ ownPeer: Raft_Peer, config: RaftConfig) {
        persistentState.ownPeer = ownPeer
        self.config = config
        logger = Logger(label: "raft.RaftNode.\(ownPeer.id)")
    }

    // MARK: - Server RPCs

    func requestVote(request: Raft_RequestVoteRequest, context: ServerContext) async throws -> Raft_RequestVoteResponse {
        logger.trace("Received request vote from \(request.candidateID)")
        resetElectionTimer()

        if request.term < persistentState.currentTerm {
            return .with { response in
                response.term = persistentState.currentTerm
                response.voteGranted = false
            }
        }

        if request.term > persistentState.currentTerm {
            logger.info("Received higher term, becoming follower")
            becomeFollower(newTerm: request.term, currentLeaderId: request.candidateID)
        }

        if !persistentState.hasVotedFor || persistentState.votedFor == request.candidateID, isLogAtLeastAsUpToDate(lastLogIndex: request.lastLogIndex, lastLogTerm: request.lastLogTerm) {
            persistentState.votedFor = request.candidateID
            return .with { response in
                response.term = persistentState.currentTerm
                response.voteGranted = true
            }
        }

        return .with { response in
            response.term = persistentState.currentTerm
            response.voteGranted = false
        }
    }

    func appendEntries(request: Raft_AppendEntriesRequest, context: ServerContext) async throws -> Raft_AppendEntriesResponse {
        .with { response in
            response.term = 0
            response.success = false
        }
    }

    func installSnapshot(request: Raft_InstallSnapshotRequest, context: ServerContext) async throws -> Raft_InstallSnapshotResponse {
        .with { response in
            response.term = 0
        }
    }

    // MARK: - State Changes

    /// Let the node stop leading.
    private func stopLeading() {
        if let heartbeatTask {
            heartbeatTask.cancel()
            self.heartbeatTask = nil
        }

        leaderState = .init()
    }

    /// Let the node become a follower.
    ///
    /// - Parameters:
    ///   - newTerm: The new term.
    ///   - currentLeaderId: The ID of the current leader.
    private func becomeFollower(newTerm: UInt64, currentLeaderId: UInt32) {
        persistentState.currentTerm = newTerm
        persistentState.clearVotedFor()
        volatileState.state = .follower
        volatileState.currentLeaderID = currentLeaderId

        stopLeading()
    }

    /// Let the node become a candidate.
    private func becomeCandidate() {
        persistentState.currentTerm += 1
        volatileState.state = .candidate
        persistentState.votedFor = id
        volatileState.clearCurrentLeaderID()

        stopLeading()
    }

    /// Let the node become a leader.
    private func becomeLeader() {
        volatileState.state = .leader
        volatileState.currentLeaderID = id

        for peer in persistentState.peers {
            leaderState.nextIndex[peer.id] = UInt64(persistentState.log.count + 1)
            leaderState.matchIndex[peer.id] = 0
        }
    }

    // MARK: - Internal

    /// Checks if the log is at least as up to date as the given log.
    ///
    /// - Parameters:
    ///   - lastLogIndex: The index of other node's last log entry.
    ///   - lastLogTerm: The term of other node's last log entry.
    private func isLogAtLeastAsUpToDate(lastLogIndex: UInt64, lastLogTerm: UInt64) -> Bool {
        let localLastLogTerm = persistentState.log.last?.term ?? 0
        let localLastLogIndex = persistentState.log.count

        if lastLogTerm != localLastLogTerm {
            return lastLogTerm > localLastLogTerm
        }

        return lastLogIndex >= localLastLogIndex
    }

    /// Resets the election timer.
    func resetElectionTimer() {
        lastHeartbeat = Date()
    }
}
