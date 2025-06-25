package raft.utils.peer

/**
 * Candidate to Peer, used for voting
 */
data class RequestVoteRequest(
    /**
     * Candidate's term
     */
    val term: Int,
    /**
     * ID of the candidate requesting vote
     */
    val candidateId: Int,
    /**
     * Index of candidate’s last log entry
     */
    val lastLogIndex: Int,
    /**
     * Term of candidate’s last log entry
     */
    val lastLogTerm: Int
)

/**
 * Peer to Candidate, response to RequestVoteRequest
 */
data class RequestVoteResponse(
    /**
     * Current term, for candidate to update itself
     */
    val term: Int,
    /**
     * True if vote granted
     */
    val voteGranted: Boolean
)
