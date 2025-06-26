package raft.transport.grpc.services

import com.google.protobuf.Empty
import raft.Client
import raft.RaftClientGrpcKt
import raft.core.node.RaftNode
import raft.core.utils.client.*
import raft.putResponse
import raft.getResponse
import raft.serverStateResponse
import raft.serverTermResponse
import raft.implementationVersionResponse
import raft.transport.grpc.utils.extensions.toGRPC

/**
 * gRPC service implementation for Raft client operations.
 */
class ClientService(
    private val node: RaftNode
) : RaftClientGrpcKt.RaftClientCoroutineImplBase() {

    override suspend fun put(request: Client.PutRequest): Client.PutResponse {
        val response = node.put(request = PutRequest(key = request.key, value = request.value))

        return putResponse {
            success = response.success
            response.leaderHint?.let { hint ->
                leaderHint = hint.toGRPC()
            }
        }
    }

    override suspend fun get(request: Client.GetRequest): Client.GetResponse {
        val response = node.get(request = GetRequest(key = request.key))

        return getResponse {
            response.value?.let { value = it }
            response.leaderHint?.let { hint ->
                leaderHint = hint.toGRPC()
            }
        }
    }

    override suspend fun getDebug(request: Client.GetRequest): Client.GetResponse {
        val response = node.getDebug(request = GetRequest(key = request.key))

        return getResponse {
            response.value?.let { value = it }
            response.leaderHint?.let { hint ->
                leaderHint = hint.toGRPC()
            }
        }
    }

    override suspend fun getServerState(request: Empty): Client.ServerStateResponse {
        val response = node.getState()

        return serverStateResponse {
            id = response.id
            state = response.state.toGRPC()
        }
    }

    override suspend fun getServerTerm(request: Empty): Client.ServerTermResponse {
        val response = node.getTerm()

        return serverTermResponse {
            term = response.term.toLong()
            id = response.id
        }
    }

    override suspend fun getImplementationVersion(request: Empty): Client.ImplementationVersionResponse {
        val response = node.getImplementationVersion()

        return implementationVersionResponse {
            id = response.id
            implementation = response.implementation
            version = response.version
        }
    }
}