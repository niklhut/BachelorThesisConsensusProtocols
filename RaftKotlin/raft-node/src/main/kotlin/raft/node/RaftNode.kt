import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import raft.node.RaftNodePersistence
import raft.node.RaftPeerTransport
import raft.types.*
import raft.types.peer.*

class RafTNode(
        private val ownPeer: Peer,
        peers: MutableList<Peer>,
        config: RaftConfig,
        private val transport: RaftPeerTransport,
        persistence: RaftNodePersistence,
) {
    private val logger: Logger = LoggerFactory.getLogger("raft.RaftNode.${ownPeer.id}")
    private val mutex = ReentrantReadWriteLock()

    private var heartbeatJob: Job? = null
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    var persistentState =
            PersistentState(
                    ownPeer = ownPeer,
                    peers = peers,
                    config = config,
                    persistence = persistence,
            )

    var volatileState = VolatileState()
    var leaderState = LeaderState()

    val majority: Int
        get() = (persistentState.peers.size + 1) / 2 + 1

    // region Server RPCs

    /**
     * Handles a RequestVote RPC.
     *
     * @param request The RequestVoteRequest to handle.
     * @return The RequestVoteResponse.
     */
    fun requestVote(request: RequestVoteRequest): RequestVoteResponse {
        logger.trace(
                "Received request vote from ${request.candidateId}, term ${request.term}, myTerm ${persistentState.currentTerm}"
        )
        resetElectionTimer()

        if (request.term < persistentState.currentTerm) {
            logger.info(
                    "Received lower term ${request.term}, not voting for ${request.candidateId}"
            )
            return RequestVoteResponse(
                    term = persistentState.currentTerm,
                    voteGranted = false,
            )
        } else if (request.term > persistentState.currentTerm) {
            logger.info(
                    "Received higher term ${request.term}, becoming follower of ${request.candidateId}"
            )
            becomeFollower(newTerm = request.term, currentLeaderId = request.candidateId)
        }

        if (persistentState.votedFor == null ||
                        persistentState.votedFor == request.candidateId &&
                                isLogAtLeastAsUpToDate(
                                        lastLogIndex = request.lastLogIndex,
                                        lastLogTerm = request.lastLogTerm
                                )
        ) {
            logger.trace("Granting vote to ${request.candidateId}")
            persistentState.votedFor = request.candidateId

            return RequestVoteResponse(
                    term = persistentState.currentTerm,
                    voteGranted = true,
            )
        }

        return RequestVoteResponse(
                term = persistentState.currentTerm,
                voteGranted = false,
        )
    }

    /**
     * Handles an AppendEntries RPC.
     *
     * @param request The AppendEntriesRequest to handle.
     * @return The AppendEntriesResponse.
     */
    fun appendEntries(request: AppendEntriesRequest): AppendEntriesResponse {
        logger.trace("Received append entries from ${request.leaderId}")

        mutex.writeLock().withLock {
            resetElectionTimer()

            if (request.term < persistentState.currentTerm) {
                return AppendEntriesResponse(
                        term = persistentState.currentTerm,
                        success = false,
                )
            } else if (request.term > persistentState.currentTerm) {
                logger.info(
                        "Received higher term ${request.term}, becoming follower of ${request.leaderId}"
                )
                becomeFollower(newTerm = request.term, currentLeaderId = request.leaderId)
            } else if (volatileState.state != ServerState.CANDIDATE) {
                // Own term and term of leader are the same
                // If the node is a candidate, it should become a follower
                becomeFollower(
                        newTerm = persistentState.currentTerm,
                        currentLeaderId = request.leaderId
                )
            } else if (volatileState.currentLeaderID != request.leaderId) {
                logger.info(
                        "Received append entries from a different leader, becoming follower of ${request.leaderId}"
                )
                becomeFollower(
                        newTerm = persistentState.currentTerm,
                        currentLeaderId = request.leaderId
                )
            }


            // Reply false if log doesn't contain an entry at prevLogIndex whose term matches
            // prevLogTerm
            if (request.prevLogIndex > 0) {
                if (persistentState.logLength < request.prevLogIndex) {
                    logger.info(
                            "Log is too short (length: ${persistentState.logLength}, needed: ${request.prevLogIndex})"
                    )
                    return AppendEntriesResponse(
                            term = persistentState.currentTerm,
                            success = false,
                    )
                }

                val prevLogTerm = if (request.prevLogIndex > persistentState.snapshot.lastIncludedIndex) {
                    persistentState.log[request.prevLogIndex - persistentState.snapshot.lastIncludedIndex - 1].term
                } else {
                    persistentState.snapshot.lastIncludedTerm
                }

                if (prevLogTerm != request.prevLogTerm) {
                    logger.info(
                            "Term mismatch at prevLogIndex ${request.prevLogIndex} (expected: $prevLogTerm, actual: ${request.prevLogTerm})"
                    )
                    return AppendEntriesResponse(
                            term = persistentState.currentTerm,
                            success = false,
                    )
                }
            }

            // If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it
            if (request.entries.isNotEmpty()) {
                var conflictIndex: Int? = null

                for ((i, newEntry) in request.entries.withIndex()) {
                    val logIndex = request.prevLogIndex + i + 1 // The absolute index of the log entry
                    val arrayIndex = logIndex - persistentState.snapshot.lastIncludedIndex - 1 // The index in the log array

                    if (arrayIndex >= 0 && arrayIndex < persistentState.log.size) {
                        // This is an existing entry in our log - check for conflict
                        val existingEntry = persistentState.log[arrayIndex]
                        if (existingEntry.term != newEntry.term) {
                            // Found a conflict - different term for same index
                            logger.info("Found conflict at index $logIndex: existing term ${existingEntry.term}, new term ${newEntry.term}")
                            conflictIndex = i
                            break
                        }
                    } else if (arrayIndex < 0) {
                        // The new entry's log index is covered by the snapshot, which means a conflict.
                        // This can happen if the leader's snapshot is older than ours,
                        // or if it's trying to send entries that are already in our snapshot.
                        logger.info("New entry at index $logIndex is already covered by snapshot.")
                        conflictIndex = i
                    } else {
                        // We've reached the end of our log - remaining entries are new
                        break
                    }
                }

                if (conflictIndex != null) {
                    // Remove conflicting entry and everything that follows
                    val deleteFromIndex = request.prevLogIndex + conflictIndex + 1 // The absolute index of the log entry
                    val deleteFromArrayIndex = deleteFromIndex - persistentState.snapshot.lastIncludedIndex - 1 // The index in the log array

                    if (deleteFromArrayIndex >= 0) {
                        logger.info("Truncating log from absolute index $deleteFromIndex (array index $deleteFromArrayIndex)")
                        persistentState.log.subList(deleteFromArrayIndex, persistentState.log.size).clear()
                    } else {
                        // The conflict is within the snapshot range or immediately after.
                        // In this case, we should discard the entire log because our log is inconsistent
                        // with what the leader is sending regarding its snapshot base.
                        logger.info("Truncating entire log due to conflict detected within or immediately after snapshot range.")
                        // TODO: check if we should do this or remove it (also in other languages)
                        persistentState.log.clear()
                    }
                }

                // Append any new entries not already in the log
                val startAppendIndex = maxOf(0, persistentState.logLength - request.prevLogIndex)
                if (startAppendIndex < request.entries.size) {
                    val entriesToAppend = request.entries.subList(startAppendIndex, request.entries.size)
                    logger.debug("Appending ${entriesToAppend.size} entries starting from log index ${persistentState.logLength + 1}")
                    persistentState.log.addAll(entriesToAppend)
                }
            }

            // Update commit index
            if (request.leaderCommit > volatileState.commitIndex) {
                val lastLogIndex = persistentState.logLength
                volatileState.commitIndex = minOf(request.leaderCommit, lastLogIndex)
                logger.debug("Updating commit index to ${volatileState.commitIndex}")

                applyCommittedEntries()
            }

            return AppendEntriesResponse(
                    term = persistentState.currentTerm,
                    success = true,
            )
        }
    }

    /**
     * Handles an InstallSnapshot RPC.
     *
     * @param request The InstallSnapshotRequest to handle.
     * @return The InstallSnapshotResponse.
     */
    suspend fun installSnapshot(request: InstallSnapshotRequest): InstallSnapshotResponse {
        logger.trace("Received snapshot to install from ${request.leaderId}")

        mutex.writeLock().withLock {
            resetElectionTimer()

            // Reply immediately if term < currentTerm
            if (request.term < persistentState.currentTerm) {
                return InstallSnapshotResponse(term = persistentState.currentTerm)
            } else if (request.term > persistentState.currentTerm) {
                logger.info("Received higher term ${request.term}, becoming follower of ${request.leaderId}")
                becomeFollower(newTerm = request.term, currentLeaderId = request.leaderId)
            } else if (volatileState.state == ServerState.CANDIDATE) {
                // Own term and term of leader are the same
                // If the node is a candidate, it should become a follower
                becomeFollower(newTerm = persistentState.currentTerm, currentLeaderId = request.leaderId)
            } else if (volatileState.currentLeaderID != request.leaderId) {
                logger.info("Received append entries from a different leader, becoming follower of ${request.leaderId}")
                becomeFollower(newTerm = persistentState.currentTerm, currentLeaderId = request.leaderId)
            }

            // Save snapshot
            val oldSnapshotLastIndex = persistentState.snapshot.lastIncludedIndex
            persistentState.persistence.saveSnapshot(request.snapshot, persistentState.ownPeer.id)
            persistentState.snapshot = request.snapshot

            val snapshotLastIndex = request.snapshot.lastIncludedIndex
            val snapshotLastTerm = request.snapshot.lastIncludedTerm

            // Update log
            if (oldSnapshotLastIndex < snapshotLastIndex) {
                val logIndex = oldSnapshotLastIndex + persistentState.log.size - snapshotLastIndex

                if (logIndex >= 0 && persistentState.log[logIndex].term == snapshotLastTerm) {
                    logger.info("Kept $logIndex log entries after installing snapshot")
                    persistentState.log.subList(0, logIndex).clear()
                } else {
                    logger.info("Discarded entire log due to conflict")
                    persistentState.log.clear()
                }
            } else {
                // Discard entire log since the new snapshot is older
                logger.info("Discarded entire log since new snapshot is older")
                persistentState.log.clear()
            }

            // Reset state machine using the snapshot
            persistentState.stateMachine = request.snapshot.stateMachine

            // Update commit and last applied indices to reflect the snapshot's state
            volatileState.commitIndex = snapshotLastIndex
            volatileState.lastApplied = snapshotLastIndex

            logger.info("Successfully installed snapshot")
            return InstallSnapshotResponse(term = persistentState.currentTerm)
        }
    }

    // endregion

    // region Election

    /** Resets the election timer. */
    private fun resetElectionTimer() {
        volatileState.lastHeartbeat = System.currentTimeMillis()
    }

    /** Checks if the election timeout has been reached. */
    private suspend fun checkElectionTimeout() {
        mutex.readLock().lock()
        val lastHeartbeat = volatileState.lastHeartbeat
        val electionTimeout = volatileState.electionTimeout
        mutex.readLock().unlock()

        val now = System.currentTimeMillis()
        if (now - lastHeartbeat > electionTimeout) {
            logger.info("Election timeout reached, becoming candidate")
            startElection()
        }
    }

    /** Starts an election. */
    private suspend fun startElection() {
        logger.trace("Starting election")
        mutex.writeLock().withLock {
            becomeCandidate()

            // Reset election timeout
            volatileState.electionTimeout = persistentState.config.electionTimeoutRange.random()
            resetElectionTimer()
        }

        // Request votes from other nodes
        requestVotes()
    }

    /** Requests votes from all peers. */
    private suspend fun requestVotes() {
        mutex.readLock().lock()
        logger.trace("Requesting votes from peers: ${persistentState.peers}")

        // Count own vote
        var votes = 1
        val requiredVotes = majority

        val currentTerm = persistentState.currentTerm
        val candidateId = persistentState.ownPeer.id
        val lastLogIndex = persistentState.logLength
        val lastLogTerm =
                persistentState.log.lastOrNull()?.term ?: persistentState.snapshot.lastIncludedTerm
        val peers = persistentState.peers
        mutex.readLock().unlock()

        coroutineScope {
            val voteResults =
                    peers.map { peer ->
                        async {
                            requestVoteFromPeer(
                                    peer = peer,
                                    voteRequest =
                                            RequestVoteRequest(
                                                    term = currentTerm,
                                                    candidateId = candidateId,
                                                    lastLogIndex = lastLogIndex,
                                                    lastLogTerm = lastLogTerm,
                                            ),
                            )
                        }
                    }

            for (voteResult in voteResults) {
                val (peerId, vote) = voteResult.await()
                logger.trace("Received vote from $peerId: ${vote.voteGranted}")

                mutex.writeLock().withLock {
                    // Check if the peer has a higher term
                    if (vote.term > persistentState.currentTerm) {
                        logger.info(
                                "Received higher term ${vote.term}, becoming follower of $peerId"
                        )
                        becomeFollower(newTerm = vote.term, currentLeaderId = peerId)
                        return@coroutineScope
                    }

                    // Count votes only if we're still a candidate and in the same term
                    if (volatileState.state == ServerState.CANDIDATE &&
                                    vote.term == persistentState.currentTerm &&
                                    vote.voteGranted
                    ) {
                        votes++

                        if (votes >= requiredVotes) {
                            logger.info(
                                    "Received majority of votes ($votes / ${peers.size + 1}), becoming leader"
                            )
                            becomeLeader()
                            return@coroutineScope
                        }
                    }
                }
            }

            mutex.readLock().withLock {
                if (volatileState.state == ServerState.CANDIDATE) {
                    logger.info("Election failed, received votes: $votes / ${peers.size + 1}")
                }
            }
        }
    }

    /**
     * Requests a vote from a peer.
     *
     * @param peer The peer to request a vote from.
     * @param voteRequest The vote request.
     * @return The vote response.
     */
    private suspend fun requestVoteFromPeer(
            peer: Peer,
            voteRequest: RequestVoteRequest,
    ): Pair<Int, RequestVoteResponse> {
        logger.trace("Requesting vote from ${peer.id}")
        return try {
            val response = transport.requestVote(voteRequest, peer)
            peer.id to response
        } catch (e: Exception) {
            logger.error("Failed to request vote from ${peer.id}", e)
            peer.id to
                    RequestVoteResponse(
                            term = 0,
                            voteGranted = false,
                    )
        }
    }

    /**
     * Checks if the log is at least as up to date as the given log.
     *
     * @param lastLogIndex The index of other node's last log entry.
     * @param lastLogTerm The term of other node's last log entry.
     * @return True if the log is at least as up to date as the given log, false otherwise.
     */
    private fun isLogAtLeastAsUpToDate(lastLogIndex: Int, lastLogTerm: Int): Boolean {
        val localLastLogTerm =
                persistentState.log.lastOrNull()?.term ?: persistentState.snapshot.lastIncludedTerm

        if (lastLogTerm != localLastLogTerm) {
            return lastLogTerm > localLastLogTerm
        }

        val localLastLogIndex = persistentState.logLength
        return lastLogIndex >= localLastLogIndex
    }

    // endregion

    // region Log Replication

    // endregion

    // region Snapshotting

    // endregion

    // region State Changes

    /**
     * Let the node become a follower.
     *
     * @param newTerm The new term.
     * @param currentLeaderId The ID of the current leader.
     */
    private fun becomeFollower(newTerm: Int, currentLeaderId: Int) {
        val oldState = volatileState.state
        if (oldState != ServerState.FOLLOWER) {
            logger.info("Transitioning from $oldState to follower for term $newTerm")
        }

        volatileState.state = ServerState.FOLLOWER
        persistentState.currentTerm = newTerm
        persistentState.votedFor = null
        volatileState.currentLeaderID = currentLeaderId
        resetElectionTimer()
    }

    /** Let the node become a candidate. */
    private fun becomeCandidate() {
        logger.info("Transitioning to candidate for term ${persistentState.currentTerm + 1}")
        volatileState.state = ServerState.CANDIDATE
        persistentState.currentTerm += 1
        persistentState.votedFor = persistentState.ownPeer.id
        volatileState.currentLeaderID = null
    }

    /** Let the node become a leader. */
    private fun becomeLeader() {
        logger.info("Transitioning to leader for term ${persistentState.currentTerm}")
        volatileState.state = ServerState.LEADER
        volatileState.currentLeaderID = persistentState.ownPeer.id

        // Clear and reinitialize maps
        leaderState.nextIndex.clear()
        leaderState.matchIndex.clear()

        // Initialize nextIndex and matchIndex for all peers
        for (peer in persistentState.peers) {
            leaderState.nextIndex[peer.id] =
                    persistentState.logLength + 1 // Next log index to send to that server
            leaderState.matchIndex[peer.id] =
                    0 // Highest log entry known to be replicated on server
        }
    }

    // endregion
}
