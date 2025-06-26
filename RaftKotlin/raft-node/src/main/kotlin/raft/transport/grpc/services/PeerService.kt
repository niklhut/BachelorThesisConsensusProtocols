package raft.transport.grpc.services

import raft.Peer
import raft.RaftPeerGrpcKt
import raft.appendEntriesResponse
import raft.core.node.RaftNode
import raft.core.utils.peer.AppendEntriesRequest
import raft.core.utils.peer.InstallSnapshotRequest
import raft.core.utils.peer.RequestVoteRequest
import raft.core.utils.types.LogEntry
import raft.core.utils.types.Snapshot
import raft.installSnapshotResponse
import raft.requestVoteResponse
import raft.transport.grpc.utils.extensions.toGRPC

/**
 * gRPC service implementation for Raft peer communication.
 */
class PeerService(
    private val node: RaftNode
) : RaftPeerGrpcKt.RaftPeerCoroutineImplBase() {

    override suspend fun requestVote(request: Peer.RequestVoteRequest): Peer.RequestVoteResponse {
        val response = node.requestVote(
            request = RequestVoteRequest(
                term = request.term.toInt(),
                candidateId = request.candidateId,
                lastLogIndex = request.lastLogIndex.toInt(),
                lastLogTerm = request.lastLogTerm.toInt()
            )
        )

        return requestVoteResponse {
            term = response.term.toLong()
            voteGranted = response.voteGranted
        }
    }

    override suspend fun appendEntries(request: Peer.AppendEntriesRequest): Peer.AppendEntriesResponse {
        val response = node.appendEntries(
            request = AppendEntriesRequest(
                term = request.term.toInt(),
                leaderId = request.leaderId,
                prevLogIndex = request.prevLogIndex.toInt(),
                prevLogTerm = request.prevLogTerm.toInt(),
                entries = request.entriesList.map { entry ->
                    LogEntry(
                        term = entry.term.toInt(),
                        key = entry.key,
                        value = entry.value
                    )
                },
                leaderCommit = request.leaderCommit.toInt()
            )
        )

        return appendEntriesResponse {
            term = response.term.toLong()
            success = response.success
        }
    }

    override suspend fun installSnapshot(request: Peer.InstallSnapshotRequest): Peer.InstallSnapshotResponse {
        val response = node.installSnapshot(
            request = InstallSnapshotRequest(
                term = request.term.toInt(),
                leaderId = request.leaderId,
                snapshot = Snapshot(
                    lastIncludedIndex = request.snapshot.lastIncludedIndex.toInt(),
                    lastIncludedTerm = request.snapshot.lastIncludedTerm.toInt(),
                    stateMachine = request.snapshot.stateMachineMap
                )
            )
        )

        return installSnapshotResponse {
            term = response.term.toLong()
        }
    }
}