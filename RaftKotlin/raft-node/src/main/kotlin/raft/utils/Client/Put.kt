package raft.utils.client

import raft.utils.types.Peer

/**
 * A request to put a key-value pair in the state machine.
 */
data class PutRequest(
    /**
     * Key to store
     */
    val key: String,
    /**
     * Optional value (absence implies a delete)
     */
    val value: String?
)

/**
 * A response to a put request.
 */
data class PutResponse(
    /**
     * Whether the operation succeeded
     */
    val success: Boolean,
    /**
     * Optional leader hint if request failed
     */
    val leaderHint: Peer? = null
)
