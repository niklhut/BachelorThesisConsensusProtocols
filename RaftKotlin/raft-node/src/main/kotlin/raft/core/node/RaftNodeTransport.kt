package raft.core.node

import raft.core.utils.peer.*
import raft.core.utils.types.Peer

/**
 * Interface for transport layer for Raft peers.
 * This allows for different transport layers to be used.
 */
interface RaftPeerTransport {
    /**
     * Sends an AppendEntries message to the specified peer.
     *
     * @param request The AppendEntries message to send.
     * @param peer The peer to send the message to.
     * @return The response from the peer.
     * @throws Exception if the message could not be sent.
     */
    @Throws(Exception::class)
    suspend fun appendEntries(
        request: AppendEntriesRequest,
        peer: Peer
    ): AppendEntriesResponse

    /**
     * Sends a RequestVote message to the specified peer.
     *
     * @param request The RequestVote message to send.
     * @param peer The peer to send the message to.
     * @return The response from the peer.
     * @throws Exception if the message could not be sent.
     */
    @Throws(Exception::class)
    suspend fun requestVote(
        request: RequestVoteRequest,
        peer: Peer
    ): RequestVoteResponse

    /**
     * Sends an InstallSnapshot message to the specified peer.
     *
     * @param request The InstallSnapshot message to send.
     * @param peer The peer to send the message to.
     * @return The response from the peer.
     * @throws Exception if the message could not be sent.
     */
    @Throws(Exception::class)
    suspend fun installSnapshot(
        request: InstallSnapshotRequest,
        peer: Peer
    ): InstallSnapshotResponse
}
