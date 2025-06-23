package raft.types.peer

import raft.types.Snapshot

/**
 * Leader to Follower, used for installing snapshots
 */
data class InstallSnapshotRequest(
    /**
     * The term of the leader
     */
    val term: Int,
    /**
     * The ID of the leader
     */
    val leaderId: Int,
    /**
     * The snapshot to install
     */
    val snapshot: Snapshot
)

/**
 * Follower to Leader, response to InstallSnapshotRequest
 */
data class InstallSnapshotResponse(
    /**
     * The term of the follower
     */
    val term: Int
)
