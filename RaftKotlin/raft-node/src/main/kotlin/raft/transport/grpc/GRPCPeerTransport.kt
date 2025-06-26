package raft.transport.grpc

import raft.core.node.RaftNodeTransport
import raft.core.utils.types.Peer
import raft.core.utils.peer.*
import raft.AppendEntriesRequestKt
import raft.AppendEntriesResponseKt
import raft.transport.grpc.utils.GRPCClientPool
import raft.RaftPeerGrpcKt
import raft.appendEntriesRequest
import raft.installSnapshotRequest
import raft.requestVoteRequest
import raft.transport.grpc.utils.extensions.*

class GRPCNodeTransport(private val clientPool: GRPCClientPool) : RaftNodeTransport {
    override suspend fun appendEntries(
            request: AppendEntriesRequest,
            peer: Peer
    ): AppendEntriesResponse {
        val client = clientPool.clientFor(peer)
        val stub = RaftPeerGrpcKt.RaftPeerCoroutineStub(client)

        val grpcRequest = appendEntriesRequest {
                term = request.term.toLong()
                leaderId = request.leaderId.toInt()
                prevLogIndex = request.prevLogIndex.toLong()
                prevLogTerm = request.prevLogTerm.toLong()
                entries.addAll(request.entries.toGRPC())
                leaderCommit = request.leaderCommit.toLong()
        }

        val grpcResponse = stub.appendEntries(grpcRequest)

        return AppendEntriesResponse(
            term = grpcResponse.term.toInt(),
            success = grpcResponse.success,
        )
    }

    override suspend fun requestVote(request: RequestVoteRequest, peer: Peer): RequestVoteResponse {
        val client = clientPool.clientFor(peer)
        val stub = RaftPeerGrpcKt.RaftPeerCoroutineStub(client)

        val grpcRequest = requestVoteRequest {
            term = request.term.toLong()
            candidateId = request.candidateId
            lastLogIndex = request.lastLogIndex.toLong()
            lastLogTerm = request.lastLogTerm.toLong()
        }

        val response = stub.requestVote(grpcRequest)

        return RequestVoteResponse(
            term = response.term.toInt(),
            voteGranted = response.voteGranted,
        )
    }

    override suspend fun installSnapshot(
            request: InstallSnapshotRequest,
            peer: Peer
    ): InstallSnapshotResponse {
        val client = clientPool.clientFor(peer)
        val stub = RaftPeerGrpcKt.RaftPeerCoroutineStub(client)

        val grpcRequest = installSnapshotRequest {
            term = request.term.toLong()
            leaderId = request.leaderId
            snapshot = request.snapshot.toGRPC()
        }

        val grpcResponse = stub.installSnapshot(grpcRequest)

        return InstallSnapshotResponse(
            term = grpcResponse.term.toInt(),
        )
    }
}
