package raft.utils.client

import raft.utils.types.ServerState

/**
 * A response to a server state request.
 */
data class ServerStateResponse(
    /**
     * The ID of the server
     */
    val id: Int,
    /**
     * The state of the server
     */
    val state: ServerState
)
