package raft.types.client

/**
 * A response to a server term request.
 */
data class ServerTermResponse(
    /**
     * The ID of the server
     */
    val id: Int,
    /**
     * The term of the server
     */
    val term: Int
)