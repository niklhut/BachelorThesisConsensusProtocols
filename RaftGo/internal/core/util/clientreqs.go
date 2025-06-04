package util

// Client to Leader, used for retrieving a value by key
type GetRequest struct {
	// Key to retrieve
	Key string `json:"key"`
}

// Leader to Client, response to GetRequest
type GetResponse struct {
	// Value associated with the key, null if not found
	Value *string `json:"value"`

	// Optional leader hint if this node is not the leader
	LeaderHint *Peer `json:"leaderHint"`
}

// Client to Leader, used for adding or updating a key-value pair
type PutRequest struct {
	// Key to store
	Key string `json:"key"`

	// Optional value (absence implies a delete)
	Value *string `json:"value"`
}

// Leader to Client, response to PutRequest
type PutResponse struct {
	// Whether the operation succeeded
	Success bool `json:"success"`

	// Optional leader hint if request failed
	LeaderHint *Peer `json:"leaderHint"`
}

// Node to Client, response to ServerStateRequest
type ServerStateResponse struct {
	// The ID of the server
	ID int `json:"id"`

	// The state of the server
	State ServerState `json:"state"`
}

// Node to Client, response to ServerTermRequest
type ServerTermResponse struct {
	// The ID of the server
	ID int `json:"id"`

	// The term of the server
	Term int `json:"term"`
}

type ImplementationVersionResponse struct {
	// The ID of the server
	ID int `json:"id"`

	// The implementation version of the server
	Implementation string `json:"implementation"`

	// The version of the Raft implementation
	Version string `json:"version"`
}
