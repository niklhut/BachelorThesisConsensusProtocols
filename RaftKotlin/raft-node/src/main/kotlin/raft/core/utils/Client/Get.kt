package raft.core.utils.client

import raft.core.utils.types.Peer

/**
 * A request to get a value by key from the state machine.
 */
data class GetRequest(
    /**
     * Key to retrieve
     */
    val key: String
)

/**
 * A response to a get request.
 */
data class GetResponse(
    /**
     * Value associated with the key, null if not found
     */
    val value: String? = null,

    /**
     * Optional leader hint if this node is not the leader
     */
    val leaderHint: Peer? = null
)
