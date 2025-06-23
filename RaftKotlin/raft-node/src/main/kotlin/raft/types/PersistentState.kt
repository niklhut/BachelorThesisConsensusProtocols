package raft.types

import raft.types.Snapshot
import raft.node.RaftNodePersistence

/**
 * The durable state persisted to disk across crashes
 */
data class PersistentState(
    /**
     * Latest term server has seen (initialized to 0)
     */
    val currentTerm: Int,
    /**
     * Candidate ID that received vote in current term (or null)
     */
    val votedFor: Int?
    /**
     * Log entries, each containing a command for the state machine
     */
    val log: MutableList<LogEntry> = mutableListOf(),
    /**
     * Latest snapshot of the state machine
     */
    val snapshot: Snapshot = Snapshot(),
    /**
     * The self peer config
     */
    val ownPeer: Peer,
    /**
     * List of peers in the cluster
     */
    val peers: MutableList<Peer>,
    /**
     * The configuration of the Raft node
     */
    val config: RaftConfig,
    /**
     * Whether the node is currently snapshotting
     */
    val isSnapshotting: Boolean = false,
    /**
     * The persistence of the node
     */
    val persistence: RaftNodePersistence
) {
    /**
     * The length of the log
     *
     * Returns the last snapshot index plus the number of entries in the log.
     */
    val logLength: Int get() = snapshot.lastIncludedIndex + log.size
}