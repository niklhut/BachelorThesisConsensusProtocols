import Foundation
import GRPCCore
import Logging

actor RaftNode: RaftNodeRPC {
    // MARK: - Properties

    // TODO: maybe move to persistent state
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
        logger.trace("Received append entries from \(request.leaderID)")
        resetElectionTimer()

        if request.term < persistentState.currentTerm {
            return .with { response in
                response.term = persistentState.currentTerm
                response.success = false
            }
        }

        if request.term > persistentState.currentTerm {
            logger.info("Received higher term, becoming follower")
            becomeFollower(newTerm: request.term, currentLeaderId: request.leaderID)
        }

        // First, update commit index
        if request.leaderCommit > volatileState.commitIndex {
            volatileState.commitIndex = min(request.leaderCommit, UInt64(persistentState.log.count))

            applyCommittedEntries()
        }

        // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if request.prevLogIndex > 0 {
            if persistentState.log.count < request.prevLogIndex {
                logger.info("Log is too short, not matching prevLogIndex")
                return .with { response in
                    response.term = persistentState.currentTerm
                    response.success = false
                }
            }

            let prevLogTerm = persistentState.log[Int(request.prevLogIndex) - 1].term
            if prevLogTerm != request.prevLogTerm {
                // Term mismatch at the expected previous index
                logger.info("Term mismatch at the expected previous index")
                return .with { response in
                    response.term = persistentState.currentTerm
                    response.success = false
                }
            }
        }

        // If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it
        // Append any new entries not already in the log
        let newEntries = request.entries
        var conflictIndex: Int? = nil

        for i in 0 ..< newEntries.count {
            let entryIndex = Int(request.prevLogIndex) + i + 1

            if entryIndex <= persistentState.log.count {
                // This is an existing entry in our log - check for conflict
                let existingTerm = persistentState.log[entryIndex - 1].term
                let newTerm = newEntries[i].term

                if existingTerm != newTerm {
                    // Found a conflict - different term for same index
                    logger.info("Found a conflict - different term for same index at \(entryIndex)")
                    conflictIndex = i
                    break
                }

                // Entry matches, will be replicated correctly
            } else {
                // We've reached the end of our log - remaining entries are new
                break
            }
        }

        if let conflictIndex {
            // Remove conflicting entry and everything that follows
            let deleteFromIndex = Int(request.prevLogIndex) + conflictIndex
            persistentState.log.removeSubrange(deleteFromIndex - 1 ..< persistentState.log.count)

            // Append new entries
            persistentState.log.append(contentsOf: newEntries[conflictIndex...])
        } else {
            // No conflict - append all new entries
            let newEntriesStartIndex = max(0, persistentState.log.count - Int(request.prevLogIndex))
            if newEntriesStartIndex < newEntries.count {
                persistentState.log.append(contentsOf: newEntries[newEntriesStartIndex...])
            }
        }

        return .with { response in
            response.term = persistentState.currentTerm
            response.success = true
        }
    }

    func installSnapshot(request: Raft_InstallSnapshotRequest, context: ServerContext) async throws -> Raft_InstallSnapshotResponse {
        .with { response in
            response.term = 0
        }
    }

    // MARK: - Log Replication

    private func applyCommittedEntries() {
        while volatileState.lastApplied < volatileState.commitIndex {
            let entry = persistentState.log[Int(volatileState.lastApplied)]
            persistentState.stateMachine[entry.key] = entry.value
            volatileState.lastApplied += 1
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
        persistentState.votedFor = persistentState.ownPeer.id
        volatileState.clearCurrentLeaderID()

        stopLeading()
    }

    /// Let the node become a leader.
    private func becomeLeader() {
        volatileState.state = .leader
        volatileState.currentLeaderID = persistentState.ownPeer.id

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
