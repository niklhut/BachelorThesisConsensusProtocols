package util

// / Leader to Follower, used for log replication and heartbeats
type AppendEntriesRequest struct {
	/// Leader's current term
	Term int `json:"term"`

	/// ID of the leader making the request
	LeaderID int `json:"leaderID"`

	/// Index of log entry immediately preceding new ones
	PrevLogIndex int `json:"prevLogIndex"`

	/// Term of prevLogIndex entry
	PrevLogTerm int `json:"prevLogTerm"`

	/// Log entries to store
	Entries []LogEntry `json:"entries"`

	/// Leader's commit index
	LeaderCommit int `json:"leaderCommit"`
}

// / Follower to Leader, response to AppendEntriesRequest
type AppendEntriesResponse struct {
	/// Current term, for leader to update itself
	Term int `json:"term"`

	/// True if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool `json:"success"`
}

// / Candidate to Peer, used for voting
type RequestVoteRequest struct {
	/// Candidate's term
	Term int `json:"term"`

	/// ID of the candidate requesting vote
	CandidateID int `json:"candidateID"`

	/// Index of candidate’s last log entry
	LastLogIndex int `json:"lastLogIndex"`

	/// Term of candidate’s last log entry
	LastLogTerm int `json:"lastLogTerm"`
}

// / Peer to Candidate, response to RequestVoteRequest
type RequestVoteResponse struct {
	/// Current term, for candidate to update itself
	Term int `json:"term"`

	/// True if vote granted
	VoteGranted bool `json:"voteGranted"`
}
