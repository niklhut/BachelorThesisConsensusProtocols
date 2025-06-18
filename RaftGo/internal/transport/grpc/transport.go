//go:generate protoc --go_out=proto --go_opt=paths=source_relative --go-grpc_out=proto --go-grpc_opt=paths=source_relative --proto_path=proto proto/types.proto proto/peer.proto proto/client.proto proto/partition.proto

package grpc

import (
	"context"
	"fmt"

	"github.com/niklhut/raft_go/internal/core/util"
	"github.com/niklhut/raft_go/internal/transport/grpc/proto"
)

type grpcRaftPeerTransport struct {
	clientPool *GRPCClientPool
}

func NewGRPCRaftPeerTransport(clientPool *GRPCClientPool) *grpcRaftPeerTransport {
	return &grpcRaftPeerTransport{
		clientPool: clientPool,
	}
}

// AppendEntries implements RaftPeerTransport.AppendEntries
func (t *grpcRaftPeerTransport) AppendEntries(
	ctx context.Context,
	req util.AppendEntriesRequest,
	to util.Peer,
) (util.AppendEntriesResponse, error) {
	client, err := t.clientPool.GetClient(to)
	if err != nil {
		return util.AppendEntriesResponse{}, fmt.Errorf("failed to get gRPC client: %w", err)
	}

	// Convert util -> protobuf
	protoReq := &proto.AppendEntriesRequest{
		Term:         uint64(req.Term),
		LeaderId:     uint32(req.LeaderID),
		PrevLogIndex: uint64(req.PrevLogIndex),
		PrevLogTerm:  uint64(req.PrevLogTerm),
		LeaderCommit: uint64(req.LeaderCommit),
	}

	for _, entry := range req.Entries {
		protoReq.Entries = append(protoReq.Entries, &proto.LogEntry{
			Term:  uint64(entry.Term),
			Key:   entry.Key,
			Value: entry.Value,
		})
	}

	protoResp, err := client.AppendEntries(ctx, protoReq)
	if err != nil {
		return util.AppendEntriesResponse{}, fmt.Errorf("rpc AppendEntries failed: %w", err)
	}

	// Convert protobuf -> util
	return util.AppendEntriesResponse{
		Term:    int(protoResp.Term),
		Success: protoResp.Success,
	}, nil
}

// RequestVote implements RaftPeerTransport.RequestVote
func (t *grpcRaftPeerTransport) RequestVote(
	ctx context.Context,
	req util.RequestVoteRequest,
	to util.Peer,
) (util.RequestVoteResponse, error) {
	client, err := t.clientPool.GetClient(to)
	if err != nil {
		return util.RequestVoteResponse{}, fmt.Errorf("failed to get gRPC client: %w", err)
	}

	protoReq := &proto.RequestVoteRequest{
		Term:         uint64(req.Term),
		CandidateId:  uint32(req.CandidateID),
		LastLogIndex: uint64(req.LastLogIndex),
		LastLogTerm:  uint64(req.LastLogTerm),
	}

	protoResp, err := client.RequestVote(ctx, protoReq)
	if err != nil {
		return util.RequestVoteResponse{}, fmt.Errorf("rpc RequestVote failed: %w", err)
	}

	return util.RequestVoteResponse{
		Term:        int(protoResp.Term),
		VoteGranted: protoResp.VoteGranted,
	}, nil
}

// InstallSnapshot implements RaftPeerTransport.InstallSnapshot
func (t *grpcRaftPeerTransport) InstallSnapshot(
	ctx context.Context,
	req util.InstallSnapshotRequest,
	to util.Peer,
) (util.InstallSnapshotResponse, error) {
	client, err := t.clientPool.GetClient(to)
	if err != nil {
		return util.InstallSnapshotResponse{}, fmt.Errorf("failed to get gRPC client: %w", err)
	}

	protoReq := &proto.InstallSnapshotRequest{
		Term:     uint64(req.Term),
		LeaderId: uint32(req.LeaderID),
		Snapshot: &proto.Snapshot{
			LastIncludedIndex: uint64(req.Snapshot.LastIncludedIndex),
			LastIncludedTerm:  uint64(req.Snapshot.LastIncludedTerm),
			StateMachine:      req.Snapshot.StateMachine,
		},
	}

	protoResp, err := client.InstallSnapshot(ctx, protoReq)
	if err != nil {
		return util.InstallSnapshotResponse{}, fmt.Errorf("rpc InstallSnapshot failed: %w", err)
	}

	return util.InstallSnapshotResponse{
		Term: int(protoResp.Term),
	}, nil
}
