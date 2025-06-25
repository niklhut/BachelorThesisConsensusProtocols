package raft.core.utils

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/** Tracks the replication of log entries to peers. */
class ReplicationTracker(
        /** The number of peers that need to have replicated the log entries. */
        private val majority: Int
) {
    /** The mutex for thread-safe access to the state. */
    private val mutex = Mutex()
    /** The set of peers that have successfully replicated the log entries. */
    private val successful: MutableSet<Int> = mutableSetOf()
    /**
     * The set of continuations to be resumed when the majority of peers have replicated the log
     * entries.
     */
    private val continuations: MutableList<CompletableDeferred<Unit>> = mutableListOf()

    /**
     * Marks a peer as successful.
     *
     * @param id The id of the peer that has successfully replicated the log entries.
     */
    suspend fun markSuccess(id: Int) {
        mutex.withLock {
            if (!successful.add(id)) {
                return
            }
            if (successful.size >= majority) {
                for (continuation in continuations) {
                    continuation.complete(Unit)
                }
                continuations.clear()
            }
        }
    }

    /** Waits for the majority of peers to have replicated the log entries. */
    suspend fun waitForMajority() {
        mutex.withLock {
            if (successful.size >= majority) {
                return
            }

            val continuation = CompletableDeferred<Unit>()
            continuations.add(continuation)
            mutex.unlock() // Avoid deadlock
            continuation.await()
            mutex.lock()
        }
    }

    /**
     * Returns whether a peer has successfully replicated the log entries.
     *
     * @param id The id of the peer to check.
     * @return True if the peer has successfully replicated the log entries, false otherwise.
     */
    suspend fun isSuccessful(id: Int): Boolean {
        return mutex.withLock { id in successful }
    }

    /**
     * Returns the set of peers that have successfully replicated the log entries.
     *
     * @return The set of peers that have successfully replicated the log entries.
     */
    suspend fun getSuccessfulPeers(): Set<Int> {
        return mutex.withLock { successful.toSet() }
    }
}
