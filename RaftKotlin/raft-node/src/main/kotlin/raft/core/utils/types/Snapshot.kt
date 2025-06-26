package raft.core.utils.types

import kotlinx.serialization.Serializable

/**
 * A snapshot of the state machine
 */
@Serializable
data class Snapshot(
    /**
     * Index of last log entry included in the snapshot
     */
    val lastIncludedIndex: Int = 0,
    /**
     * Term of last log entry included in the snapshot
     */
    val lastIncludedTerm: Int = 0,
    /**
     * State machine state
     */
    val stateMachine: MutableMap<String, String> = mutableMapOf()
)