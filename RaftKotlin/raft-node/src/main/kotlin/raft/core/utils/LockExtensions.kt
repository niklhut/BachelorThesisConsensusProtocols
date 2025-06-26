package raft.core.utils

import java.util.concurrent.locks.Lock

/**
 * Acquires the lock and releases it after the action is executed.
 */
inline fun <T> Lock.withLock(action: () -> T): T {
    lock()
    try {
        return action()
    } finally {
        unlock()
    }
}
