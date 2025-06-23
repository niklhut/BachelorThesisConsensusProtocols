package raft.node

import raft.types.peer.*
import raft.types.Peer

/**
 * Interface for transport layer for Raft peers.
 * This allows for different transport layers to be used.
 */
interface RaftPeerTransport {
    /**
     * Sends an AppendEntries message to the specified peer.
     * - Parameters:
     *   - request: The AppendEntries message to send.
     *   - peer: The peer to send the message to.
     * - Returns: The response from the peer.
     * - Throws: An error if the message could not be sent.
     */
    @Throws(Exception::class)
    suspend fun appendEntries(
        request: AppendEntriesRequest,
        peer: Peer
    ): AppendEntriesResponse

    /**
     * Sends a RequestVote message to the specified peer.
     * - Parameters:
     *   - request: The RequestVote message to send.
     *   - peer: The peer to send the message to.
     * - Returns: The response from the peer.
     * - Throws: An error if the message could not be sent.
     */
    @Throws(Exception::class)
    suspend fun requestVote(
        request: RequestVoteRequest,
        peer: Peer
    ): RequestVoteResponse

    /**
     * Sends an InstallSnapshot message to the specified peer.
     * - Parameters:
     *   - request: The InstallSnapshot message to send.
     *   - peer: The peer to send the message to.
     * - Returns: The response from the peer.
     * - Throws: An error if the message could not be sent.
     */
    @Throws(Exception::class)
    suspend fun installSnapshot(
        request: InstallSnapshotRequest,
        peer: Peer
    ): InstallSnapshotResponse
}
