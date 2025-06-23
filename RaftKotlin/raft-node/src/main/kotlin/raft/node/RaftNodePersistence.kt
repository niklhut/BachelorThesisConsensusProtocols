package raft.node

import raft.types.Snapshot

/**
 * Interface for persisting snapshots
 */
interface RaftNodePersistence {
    /**
     * The compaction threshold
     *
     * This is the number of entries that are stored in memory before a snapshot is taken.
     */
    val compactionThreshold: Int

    /**
     * Saves a snapshot for a node.
     *
     * @param snapshot The snapshot to save.
     * @param nodeId The id of the node to which the snapshot belongs.
     */
    @Throws(Exception::class)
    suspend fun saveSnapshot(snapshot: Snapshot, nodeId: Int)

    /**
     * Loads a snapshot.
     *
     * @param nodeId The node id to which the snapshot belongs.
     * @return The loaded snapshot, if found.
     */
    @Throws(Exception::class)
    suspend fun loadSnapshot(nodeId: Int): Snapshot?

    /**
     * Deletes a snapshot.
     *
     * @param nodeId The node id to which the snapshot belongs.
     */
    @Throws(Exception::class)
    suspend fun deleteSnapshot(nodeId: Int)
}
