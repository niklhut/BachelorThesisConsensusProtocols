package util

// RaftError represents errors specific to the Raft client.
type RaftError string

func (e RaftError) Error() string {
	return string(e)
}

const (
	NotLeaderError RaftError = "not leader"
)
