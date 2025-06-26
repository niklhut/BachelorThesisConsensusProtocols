package raft.core.node.persistence

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import raft.core.node.RaftNodePersistence
import raft.core.utils.types.Snapshot

/**
 * In memory implementation of node persistence.
 *
 * This implementation of persistence is not persistent across node failures,
 * since the snapshots are saved in memory.
 */
class InMemoryRaftNodePersistence(
    override val compactionThreshold: Int
) : RaftNodePersistence {

    private val snapshots: MutableMap<Int, Snapshot> = mutableMapOf()
    private val mutex = Mutex()

    override suspend fun saveSnapshot(snapshot: Snapshot, nodeId: Int) {
        mutex.withLock {
            snapshots[nodeId] = snapshot
        }
    }

    override suspend fun loadSnapshot(nodeId: Int): Snapshot? {
        mutex.withLock {
            return snapshots[nodeId]
        }
    }

    override suspend fun deleteSnapshot(nodeId: Int) {
        mutex.withLock {
            snapshots.remove(nodeId)
        }
    }
}