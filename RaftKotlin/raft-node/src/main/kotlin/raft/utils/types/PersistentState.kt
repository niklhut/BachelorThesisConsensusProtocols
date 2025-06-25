package raft.utils.types

import raft.utils.types.Snapshot
import raft.node.RaftNodePersistence

/**
 * The durable state persisted to disk across crashes
 */
data class PersistentState(
    /**
     * Latest term server has seen (initialized to 0)
     */
    var currentTerm: Int = 0,
    /**
     * Candidate ID that received vote in current term (or null)
     */
    var votedFor: Int? = null,
    /**
     * Log entries, each containing a command for the state machine
     */
    val log: MutableList<LogEntry> = mutableListOf(),
    /**
     * State machine state
     */
    var stateMachine: MutableMap<String, String> = mutableMapOf(),
    /**
     * Latest snapshot of the state machine
     */
    var snapshot: Snapshot = Snapshot(),
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
    var isSnapshotting: Boolean = false,
    /**
     * Whether the node is currently sending a snapshot to a peer
     */
    var isSendingSnapshot: MutableMap<Int, Boolean> = mutableMapOf(),
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