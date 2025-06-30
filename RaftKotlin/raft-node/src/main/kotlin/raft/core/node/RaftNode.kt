package raft.core.node

import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import raft.core.utils.*
import raft.core.utils.client.*
import raft.core.utils.peer.*
import raft.core.utils.types.*

class RaftNode(
        private val ownPeer: Peer,
        peers: MutableList<Peer>,
        config: RaftConfig,
        private val transport: RaftNodeTransport,
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

        mutex.writeLock().withLock {
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

                val prevLogTerm =
                        if (request.prevLogIndex > persistentState.snapshot.lastIncludedIndex) {
                            persistentState.log[
                                            request.prevLogIndex -
                                                    persistentState.snapshot.lastIncludedIndex -
                                                    1]
                                    .term
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
                    val logIndex =
                            request.prevLogIndex + i + 1 // The absolute index of the log entry
                    val arrayIndex =
                            logIndex -
                                    persistentState.snapshot.lastIncludedIndex -
                                    1 // The index in the log array

                    if (arrayIndex >= 0 && arrayIndex < persistentState.log.size) {
                        // This is an existing entry in our log - check for conflict
                        val existingEntry = persistentState.log[arrayIndex]
                        if (existingEntry.term != newEntry.term) {
                            // Found a conflict - different term for same index
                            logger.info(
                                    "Found conflict at index $logIndex: existing term ${existingEntry.term}, new term ${newEntry.term}"
                            )
                            conflictIndex = i
                            break
                        }
                    } else if (arrayIndex < 0) {
                        // The new entry's log index is covered by the snapshot, which means a
                        // conflict.
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
                    val deleteFromIndex =
                            request.prevLogIndex +
                                    conflictIndex +
                                    1 // The absolute index of the log entry
                    val deleteFromArrayIndex =
                            deleteFromIndex -
                                    persistentState.snapshot.lastIncludedIndex -
                                    1 // The index in the log array

                    if (deleteFromArrayIndex >= 0) {
                        logger.info(
                                "Truncating log from absolute index $deleteFromIndex (array index $deleteFromArrayIndex)"
                        )
                        persistentState
                                .log
                                .subList(deleteFromArrayIndex, persistentState.log.size)
                                .clear()
                    } else {
                        // The conflict is within the snapshot range or immediately after.
                        // In this case, we should discard the entire log because our log is
                        // inconsistent
                        // with what the leader is sending regarding its snapshot base.
                        logger.info(
                                "Truncating entire log due to conflict detected within or immediately after snapshot range."
                        )
                        // TODO: check if we should do this or remove it (also in other languages)
                        persistentState.log.clear()
                    }
                }

                // Append any new entries not already in the log
                val startAppendIndex = maxOf(0, persistentState.logLength - request.prevLogIndex)
                if (startAppendIndex < request.entries.size) {
                    val entriesToAppend =
                            request.entries.subList(startAppendIndex, request.entries.size)
                    logger.debug(
                            "Appending ${entriesToAppend.size} entries starting from log index ${persistentState.logLength + 1}"
                    )
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
                logger.info(
                        "Received higher term ${request.term}, becoming follower of ${request.leaderId}"
                )
                becomeFollower(newTerm = request.term, currentLeaderId = request.leaderId)
            } else if (volatileState.state == ServerState.CANDIDATE) {
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

    // region Client RPCs

    /**
     * Handles a Put RPC.
     *
     * @param request The PutRequest to handle.
     * @return The PutResponse.
     */
    public suspend fun put(request: PutRequest): PutResponse {
        mutex.readLock().lock()
        if (volatileState.state != ServerState.LEADER) {
            mutex.readLock().unlock()
            return PutResponse(
                    success = false,
                    leaderHint =
                            persistentState.peers.firstOrNull {
                                it.id == volatileState.currentLeaderID
                            }
            )
        }
        val term = persistentState.currentTerm
        mutex.readLock().unlock()

        replicateLog(
                entries = listOf(LogEntry(term = term, key = request.key, value = request.value))
        )

        return PutResponse(success = true)
    }

    /**
     * Handles a Get RPC.
     *
     * @param request The GetRequest to handle.
     * @return The GetResponse.
     */
    public suspend fun get(request: GetRequest): GetResponse {
        mutex.readLock().withLock {
            if (volatileState.state != ServerState.LEADER) {
                val leaderHint =
                        persistentState.peers.firstOrNull { it.id == volatileState.currentLeaderID }
                return GetResponse(leaderHint = leaderHint)
            }
            return GetResponse(value = persistentState.stateMachine[request.key])
        }
    }

    /**
     * Handles a GetDebug RPC.
     *
     * @param request The GetRequest to handle.
     * @return The GetResponse.
     */
    public suspend fun getDebug(request: GetRequest): GetResponse {
        mutex.readLock().withLock {
            val value = persistentState.stateMachine[request.key]
            return GetResponse(value = value)
        }
    }

    /**
     * Handles a GetState RPC.
     *
     * @return The ServerStateResponse.
     */
    public suspend fun getState(): ServerStateResponse {
        mutex.readLock().withLock {
            return ServerStateResponse(
                    id = persistentState.ownPeer.id,
                    state = volatileState.state,
            )
        }
    }

    /**
     * Handles a GetTerm RPC.
     *
     * @return The GetTermResponse.
     */
    public suspend fun getTerm(): ServerTermResponse {
        mutex.readLock().withLock {
            return ServerTermResponse(
                    id = persistentState.ownPeer.id,
                    term = persistentState.currentTerm,
            )
        }
    }

    /**
     * Handles a GetImplementationVersion RPC.
     *
     * @return The ImplementationVersionResponse.
     */
    public suspend fun getImplementationVersion(): ImplementationVersionResponse {
        mutex.readLock().withLock {
            return ImplementationVersionResponse(
                    id = persistentState.ownPeer.id,
                    implementation = "Kotlin",
                    version = "1.3.0",
            )
        }
    }

    // endregion

    // region Node Lifecycle

    /** Starts the node. */
    public suspend fun start() {
        loadSnapshotOnStartup()
        startHeartbeatTask()
    }

    /** Shuts down the node. */
    public suspend fun shutdown() {
        heartbeatJob?.cancel()
        scope.cancel()
    }

    /** Starts the heartbeat task. */
    private fun startHeartbeatTask() {
        // Cancel existing task if it exists
        heartbeatJob?.cancel()

        mutex.readLock().lock()
        val heartbeatInterval = persistentState.config.heartbeatInterval.milliseconds
        mutex.readLock().unlock()

        heartbeatJob =
                scope.launch {
                    while (currentCoroutineContext().job.isActive) {
                        try {
                            val timeout: Duration
                            val action: suspend () -> Unit

                            mutex.readLock().lock()
                            val state = volatileState.state
                            mutex.readLock().unlock()

                            if (state == ServerState.LEADER) {
                                timeout = heartbeatInterval
                                action = ::sendHeartbeat
                            } else {
                                timeout = heartbeatInterval * 3
                                action = ::checkElectionTimeout
                            }

                            val actionJob = launch { action() }

                            delay(timeout)
                            actionJob.cancel()
                        } catch (e: Exception) {
                            logger.error("Heartbeat task failed: $e")
                            continue
                        }
                    }
                }
    }

    /** Sends a heartbeat to all followers. */
    private suspend fun sendHeartbeat() {
        mutex.readLock().lock()
        val state = volatileState.state
        mutex.readLock().unlock()

        if (state != ServerState.LEADER) {
            throw RaftError.NotLeader
        }

        logger.trace("Sending heartbeat to followers")
        replicateLog(emptyList())
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
            logger.error("Failed to request vote from ${peer.id}")
            peer.id to
                    RequestVoteResponse(
                            term = 0,
                            voteGranted = false,
                    )
        }
    }

    // endregion

    // region Log Replication

    /**
     * Replicates log entries to all peers.
     *
     * @param entries The log entries to replicate.
     */
    private suspend fun replicateLog(entries: List<LogEntry>) {
        mutex.writeLock().lock()

        if (volatileState.state != ServerState.LEADER) {
            logger.info("Not a leader, not replicating log")
            mutex.writeLock().unlock()
            return
        }

        resetElectionTimer()

        val currentTerm = persistentState.currentTerm
        val leaderID = persistentState.ownPeer.id
        val peers = persistentState.peers
        val commitIndex = volatileState.commitIndex
        val originalLogLength = persistentState.log.size
        val majority = majority

        // Add log entries to log
        persistentState.log.addAll(entries)
        mutex.writeLock().unlock()

        // Create replication tracker with leader pre-marked as successful
        val replicationTracker = ReplicationTracker(majority)
        logger.trace("Replicating log entries to peers, majority: $majority, entries: $entries")
        replicationTracker.markSuccess(leaderID)

        scope.launch {
            coroutineScope {
                for (peer in peers) {
                    launch {
                        replicateLogToPeer(
                                peer = peer,
                                replicationTracker = replicationTracker,
                                currentTerm = currentTerm,
                                leaderID = leaderID,
                                commitIndex = commitIndex,
                                originalLogLength = originalLogLength,
                                entries = entries,
                        )
                    }
                }
            }
        }

        // Wait for majority to have replicated the log before returning
        replicationTracker.waitForMajority()
        logger.trace("Majority of peers have replicated the log")

        // Once majority has replicated the log, update commit index
        // and apply committed entries
        if (volatileState.state == ServerState.LEADER && persistentState.currentTerm == currentTerm
        ) {
            updateCommitIndexAndApply()
        }
    }

    /**
     * Replicates log entries to a single peer.
     *
     * @param peer The peer to replicate the log to.
     * @param replicationTracker The replication tracker.
     * @param currentTerm The current term.
     * @param leaderID The leader ID.
     * @param commitIndex The commit index.
     * @param originalLogLength The original log length.
     * @param entries The log entries to replicate.
     */
    private suspend fun replicateLogToPeer(
            peer: Peer,
            replicationTracker: ReplicationTracker,
            currentTerm: Int,
            leaderID: Int,
            commitIndex: Int,
            originalLogLength: Int,
            entries: List<LogEntry>,
    ) {
        var retryCount = 0
        val targetEndIndex = originalLogLength + entries.size

        while (!currentCoroutineContext().job.isCancelled) {
            mutex.writeLock().withLock {
                // Check conditions for continuing
                if (volatileState.state != ServerState.LEADER ||
                                persistentState.currentTerm != currentTerm ||
                                persistentState.peers.none { it.id == peer.id }
                ) {
                    return
                }
            }

            // Check if already successful
            if (replicationTracker.isSuccessful(peer.id)) {
                return
            }

            mutex.writeLock().lock()
            val currentMatchIndex = leaderState.matchIndex[peer.id]
            if (currentMatchIndex != null && currentMatchIndex > targetEndIndex) {
                mutex.writeLock().unlock()
                replicationTracker.markSuccess(peer.id)
                return
            }

            // Determine peer's next index
            val peerNextIndex =
                    leaderState.nextIndex[peer.id] ?: persistentState.snapshot.lastIncludedIndex + 1

            // Check if peer needs a snapshot
            if (peerNextIndex <= persistentState.snapshot.lastIncludedIndex) {
                mutex.writeLock().unlock()
                // Peer is too far behind, send snapshot
                logger.info(
                        "Peer ${peer.id} is too far behind (nextIndex: ${peerNextIndex}), sending snapshot. Snapshot last included index: ${persistentState.snapshot.lastIncludedIndex}"
                )

                sendSnapshotToPeer(peer)

                mutex.writeLock().withLock {
                    // After sending snapshot, update nextIndex and matchIndex
                    leaderState.nextIndex[peer.id] = persistentState.snapshot.lastIncludedIndex + 1
                    leaderState.matchIndex[peer.id] = persistentState.snapshot.lastIncludedIndex
                }

                // Continue to next iteration to try sending append entries from the new nextIndex
                continue
            }

            try {
                // Calculate entries to send
                val entriesToSend: List<LogEntry>
                if (peerNextIndex <= originalLogLength) {
                    // Peer needs entries from the original log (catch-up scenario)
                    val startIndex = peerNextIndex - persistentState.snapshot.lastIncludedIndex - 1
                    entriesToSend =
                            persistentState.log.subList(startIndex, persistentState.log.size)
                } else if (peerNextIndex == originalLogLength + 1) {
                    // Peer is up-to-date with original log, send only new entries
                    entriesToSend = entries
                } else {
                    // Peer's nextIndex is beyond what we expect - this shouldn't happen
                    // Reset nextIndex and retry
                    logger.warn(
                            "Peer ${peer.id} has nextIndex $peerNextIndex which is beyond what we expect (originalLogLength: $originalLogLength)"
                    )
                    leaderState.nextIndex[peer.id] = originalLogLength + 1
                    continue
                }

                val peerPrevLogIndex = peerNextIndex - 1
                val peerPrevLogTerm =
                        if (peerPrevLogIndex > persistentState.snapshot.lastIncludedIndex) {
                            persistentState.log[
                                            peerPrevLogIndex -
                                                    persistentState.snapshot.lastIncludedIndex -
                                                    1]
                                    .term
                        } else {
                            persistentState.snapshot.lastIncludedTerm
                        }

                mutex.writeLock().unlock()

                logger.trace(
                        "Sending append entries to ${peer.id} with nextIndex: $peerNextIndex, prevLogIndex: $peerPrevLogIndex, prevLogTerm: $peerPrevLogTerm, entries.count: ${entriesToSend.size}"
                )

                val appendEntriesRequest =
                        AppendEntriesRequest(
                                term = currentTerm,
                                leaderId = leaderID,
                                prevLogIndex = peerPrevLogIndex,
                                prevLogTerm = peerPrevLogTerm,
                                entries = entriesToSend,
                                leaderCommit = commitIndex,
                        )

                val result = transport.appendEntries(appendEntriesRequest, peer)

                if (currentCoroutineContext().job.isCancelled) {
                    return
                }

                mutex.writeLock().lock()

                if (result.term > persistentState.currentTerm) {
                    logger.info(
                            "Received higher term ${result.term}, becoming follower of ${peer.id}"
                    )
                    becomeFollower(newTerm = result.term, currentLeaderId = peer.id)
                    mutex.writeLock().unlock()
                    return
                }

                if (result.success) {
                    replicationTracker.markSuccess(peer.id)
                    logger.trace("Append entries successful for peer ${peer.id}")

                    val newMatchIndex = peerPrevLogIndex + entriesToSend.size
                    leaderState.matchIndex[peer.id] = newMatchIndex
                    leaderState.nextIndex[peer.id] = newMatchIndex + 1

                    mutex.writeLock().unlock()
                    return
                } else {
                    // Log inconsistency, decrement nextIndex and retry
                    leaderState.nextIndex[peer.id] =
                            maxOf(1, leaderState.nextIndex[peer.id] ?: 1 - 1)
                    mutex.writeLock().unlock()

                    retryCount += 1
                    logger.info(
                            "Append entries failed for ${peer.id}, retrying with earlier index, retrying with index ${leaderState.nextIndex[peer.id]}, retryCount: $retryCount, nextIndex: ${leaderState.nextIndex[peer.id]}"
                    )

                    // Wait before retrying with exponential backoff
                    delay(100L * minOf(64, 1 shl retryCount))
                }
            } catch (e: Exception) {
                val errorMessage =
                        "Append entries failed for ${peer.id}, retrying with earlier index, retrying with index ${leaderState.nextIndex[peer.id]}, retryCount: $retryCount, nextIndex: ${leaderState.nextIndex[peer.id]}"
                if (retryCount == 0) {
                    logger.error(errorMessage)
                } else {
                    logger.trace(errorMessage)
                }
                retryCount++

                // Wait before retrying with exponential backoff
                delay(100L * minOf(64, 1 shl retryCount))
            }
        }
    }

    /** Updates the commit index and applies the committed entries to the state machine. */
    private suspend fun updateCommitIndexAndApply() {
        mutex.readLock().lock()

        if (volatileState.state != ServerState.LEADER) {
            mutex.readLock().unlock()
            return
        }

        // Add own match index (implicitly the end of the log)
        val allMatchIndices = mutableMapOf<Int, Int>()
        allMatchIndices.putAll(leaderState.matchIndex)
        allMatchIndices[persistentState.ownPeer.id] = persistentState.logLength

        mutex.readLock().unlock()

        // Calculate new commit index based on majority match indices
        val indices = allMatchIndices.values.toList().sortedDescending()
        val majorityIndex = indices[majority - 1]

        mutex.writeLock().withLock {
            // Only update commit index if it's in the current term
            val oldCommitIndex = volatileState.commitIndex

            // Start from the majority index and work backwards to find the highest
            // committable index in the current term
            for (newCommitIndex in majorityIndex downTo oldCommitIndex + 1) {
                // Check if this index is within the snapshot range
                if (newCommitIndex <= persistentState.snapshot.lastIncludedIndex) {
                    // This index is covered by the snapshot, so it's already committed
                    // Don't update commitIndex as it should already be at least this value
                    break
                }

                // Calculate the array index in the current log
                val logArrayIndex = newCommitIndex - persistentState.snapshot.lastIncludedIndex - 1

                // Ensure the index is within bounds of the current log
                if (logArrayIndex >= 0 && logArrayIndex < persistentState.log.size) {
                    val entry = persistentState.log[logArrayIndex]
                    // Only commit entries from the current term for safety
                    if (entry.term == persistentState.currentTerm) {
                        volatileState.commitIndex = newCommitIndex
                        logger.trace("Updated commit index from $oldCommitIndex to $newCommitIndex")
                        break
                    }
                    // If the entry is from an older term, continue checking lower indices
                } else {
                    // Index is out of bounds - this shouldn't happen with correct logic
                    logger.warn(
                            "Calculated newCommitIndex $newCommitIndex is out of bounds for log (length: ${persistentState.log.size}, snapshot last index: ${persistentState.snapshot.lastIncludedIndex})"
                    )
                }
            }

            applyCommittedEntries()
        }
    }

    /** Applies the committed entries to the state machine. */
    private fun applyCommittedEntries() {
        while (volatileState.lastApplied < volatileState.commitIndex) {
            val nextApplyIndex = volatileState.lastApplied + 1

            if (nextApplyIndex <= persistentState.snapshot.lastIncludedIndex) {
                volatileState.lastApplied = nextApplyIndex
                continue
            }

            val logArrayIndex = nextApplyIndex - persistentState.snapshot.lastIncludedIndex - 1

            if (logArrayIndex >= 0 && logArrayIndex < persistentState.log.size) {
                val entry = persistentState.log[logArrayIndex]

                if (entry.key != null) {
                    val oldValue = persistentState.stateMachine[entry.key]
                    if (entry.value != null) {
                        persistentState.stateMachine[entry.key] = entry.value
                    } else {
                        persistentState.stateMachine.remove(entry.key)
                    }
                    logger.trace(
                            "Applied entry at index $nextApplyIndex: ${entry.key} = ${entry.value} (was: $oldValue)"
                    )
                }

                volatileState.lastApplied += 1
            } else {
                logger.warn(
                        "Cannot apply entry at index $nextApplyIndex: log array index $logArrayIndex is out of bounds (log size: ${persistentState.log.size}, snapshot last index: ${persistentState.snapshot.lastIncludedIndex})"
                )
                break
            }
        }

        if (shouldCreateSnapshot()) {
            scope.launch { createSnapshot() }
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

    // region Snapshotting

    /** Loads the snapshot on startup of the node. */
    private suspend fun loadSnapshotOnStartup() {
        try {
            mutex.writeLock().withLock {
                val snapshot = persistentState.persistence.loadSnapshot(persistentState.ownPeer.id)
                if (snapshot != null) {
                    persistentState.snapshot = snapshot
                    persistentState.currentTerm = snapshot.lastIncludedTerm
                    persistentState.stateMachine = snapshot.stateMachine
                    volatileState.commitIndex = snapshot.lastIncludedIndex
                    volatileState.lastApplied = snapshot.lastIncludedIndex
                    logger.info(
                            "Successfully loaded snapshot up to index ${snapshot.lastIncludedIndex}."
                    )
                }
            }
        } catch (e: Exception) {
            logger.error("Failed to load snapshot: ${e.message}")
        }
    }

    /**
     * Checks if a snapshot should be created.
     *
     * @return True if a snapshot should be created, false otherwise.
     */
    private fun shouldCreateSnapshot(): Boolean {
        mutex.readLock().withLock {
            if (persistentState.persistence.compactionThreshold <= 0) {
                return false
            }

            return (volatileState.commitIndex - persistentState.snapshot.lastIncludedIndex) >=
                    persistentState.persistence.compactionThreshold &&
                    !persistentState.isSnapshotting
        }
    }

    /** Creates a snapshot of the state machine. */
    private suspend fun createSnapshot() {
        mutex.writeLock().withLock {
            if (persistentState.isSnapshotting) {
                return
            }

            persistentState.isSnapshotting = true

            val snapshotLastIndex = volatileState.commitIndex
            if (snapshotLastIndex <= persistentState.snapshot.lastIncludedIndex) {
                persistentState.isSnapshotting = false
                return
            }

            val lastCommittedArrayIndex =
                    snapshotLastIndex - persistentState.snapshot.lastIncludedIndex - 1
            if (lastCommittedArrayIndex < 0 || lastCommittedArrayIndex >= persistentState.log.size
            ) {
                persistentState.isSnapshotting = false
                return
            }

            val snapshotLastTerm = persistentState.log[lastCommittedArrayIndex].term

            // Deep copy the state machine to avoid concurrent access
            val stateMachineCopy = mutableMapOf<String, String>()
            stateMachineCopy.putAll(persistentState.stateMachine)

            val snapshot =
                    Snapshot(
                            lastIncludedIndex = snapshotLastIndex,
                            lastIncludedTerm = snapshotLastTerm,
                            stateMachine = stateMachineCopy,
                    )

            val ownId = persistentState.ownPeer.id

            mutex.writeLock().unlock()
            persistentState.persistence.saveSnapshot(snapshot, ownId)
            mutex.writeLock().lock()

            persistentState.snapshot = snapshot

            // Truncate the log after saving snapshot
            val entriesToKeep = lastCommittedArrayIndex + 1
            persistentState.log.subList(0, entriesToKeep).clear()

            persistentState.isSnapshotting = false

            logger.info(
                    "Successfully created snapshot",
                    "lastIncludedIndex" to snapshotLastIndex,
                    "lastIncludedTerm" to snapshotLastTerm
            )
        }
    }

    /**
     * Sends a snapshot to a specific peer.
     *
     * @param peer The peer to send the snapshot to.
     */
    private suspend fun sendSnapshotToPeer(peer: Peer) {
        mutex.readLock().lock()

        if (volatileState.state != ServerState.LEADER ||
                        persistentState.isSendingSnapshot[peer.id] == true
        ) {
            mutex.readLock().unlock()
            return
        }

        val currentTerm = persistentState.currentTerm
        val leaderId = persistentState.ownPeer.id
        val snapshot = persistentState.snapshot
        mutex.readLock().unlock()
        mutex.writeLock().lock()
        persistentState.isSendingSnapshot[peer.id] = true
        mutex.writeLock().unlock()

        logger.info(
                "Sending InstallSnapshot RPC to $peer.id (snapshot last index: ${snapshot.lastIncludedIndex}, last term: ${snapshot.lastIncludedTerm})"
        )

        val request =
                InstallSnapshotRequest(
                        term = currentTerm,
                        leaderId = leaderId,
                        snapshot = snapshot,
                )

        try {
            val response = transport.installSnapshot(request, peer)

            mutex.writeLock().withLock {
                persistentState.isSendingSnapshot[peer.id] = false

                if (response.term > persistentState.currentTerm) {
                    logger.info(
                            "Received higher term ${response.term}, becoming follower of ${peer.id}"
                    )
                    becomeFollower(response.term, peer.id)
                    return
                }

                // Upon successful installation, update matchIndex and nextIndex for the peer
                leaderState.matchIndex[peer.id] = snapshot.lastIncludedIndex
                leaderState.nextIndex[peer.id] = snapshot.lastIncludedIndex + 1
                logger.info(
                        "Successfully sent snapshot to $peer.id. Updated nextIndex to ${leaderState.nextIndex[peer.id] ?: 0} and matchIndex to ${leaderState.matchIndex[peer.id] ?: 0}"
                )
            }
        } catch (e: Exception) {
            logger.error("Failed to send snapshot to $peer.id: ${e.message}")
            mutex.writeLock().withLock { persistentState.isSendingSnapshot[peer.id] = false }
        }
    }

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
