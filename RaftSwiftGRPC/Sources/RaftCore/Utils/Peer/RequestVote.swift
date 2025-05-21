/// Candidate to Peer, used for voting
public struct RequestVoteRequest: Sendable, Codable {
    /// Candidate's term
    public let term: Int

    /// ID of the candidate requesting vote
    public let candidateID: Int

    /// Index of candidate’s last log entry
    public let lastLogIndex: Int

    /// Term of candidate’s last log entry
    public let lastLogTerm: Int

    /// Creates a new RequestVoteRequest
    /// - Parameters:
    ///   - term: Candidate's term
    ///   - candidateID: ID of the candidate requesting vote
    ///   - lastLogIndex: Index of candidate’s last log entry
    ///   - lastLogTerm: Term of candidate’s last log entry
    public init(
        term: Int,
        candidateID: Int,
        lastLogIndex: Int,
        lastLogTerm: Int
    ) {
        self.term = term
        self.candidateID = candidateID
        self.lastLogIndex = lastLogIndex
        self.lastLogTerm = lastLogTerm
    }
}

/// Peer to Candidate, response to RequestVoteRequest
public struct RequestVoteResponse: Sendable, Codable {
    /// Current term, for candidate to update itself
    public let term: Int

    /// True if vote granted
    public let voteGranted: Bool

    /// Creates a new RequestVoteResponse
    /// - Parameters:
    ///   - term: Current term of the peer
    ///   - voteGranted: True if vote granted
    public init(
        term: Int,
        voteGranted: Bool
    ) {
        self.term = term
        self.voteGranted = voteGranted
    }
}
