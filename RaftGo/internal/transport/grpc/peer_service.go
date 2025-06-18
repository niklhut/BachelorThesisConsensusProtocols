package grpc

import (
	"context"

	"github.com/niklhut/raft_go/internal/core/node"
	"github.com/niklhut/raft_go/internal/core/util"
	"github.com/niklhut/raft_go/internal/transport/grpc/proto"
)

type RaftPeerService struct {
	proto.UnimplementedRaftPeerServer
	node *node.RaftNode
}

func NewRaftPeerService(node *node.RaftNode) *RaftPeerService {
	return &RaftPeerService{node: node}
}

func (s *RaftPeerService) AppendEntries(
	ctx context.Context,
	req *proto.AppendEntriesRequest,
) (*proto.AppendEntriesResponse, error) {
	entries := make([]util.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = util.LogEntry{
			Term:  int(e.Term),
			Key:   e.Key,
			Value: e.Value,
		}
	}

	resp := s.node.HandleAppendEntries(ctx, util.AppendEntriesRequest{
		Term:         int(req.Term),
		LeaderID:     int(req.LeaderId),
		PrevLogIndex: int(req.PrevLogIndex),
		PrevLogTerm:  int(req.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int(req.LeaderCommit),
	})

	return &proto.AppendEntriesResponse{
		Term:    uint64(resp.Term),
		Success: resp.Success,
	}, nil
}

func (s *RaftPeerService) RequestVote(
	ctx context.Context,
	req *proto.RequestVoteRequest,
) (*proto.RequestVoteResponse, error) {
	resp := s.node.HandleRequestVote(ctx, util.RequestVoteRequest{
		Term:         int(req.Term),
		CandidateID:  int(req.CandidateId),
		LastLogIndex: int(req.LastLogIndex),
		LastLogTerm:  int(req.LastLogTerm),
	})

	return &proto.RequestVoteResponse{
		Term:        uint64(resp.Term),
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (s *RaftPeerService) InstallSnapshot(
	ctx context.Context,
	req *proto.InstallSnapshotRequest,
) (*proto.InstallSnapshotResponse, error) {
	resp := s.node.HandleInstallSnapshot(util.InstallSnapshotRequest{
		Term:     int(req.Term),
		LeaderID: int(req.LeaderId),
		Snapshot: util.Snapshot{
			LastIncludedIndex: int(req.Snapshot.LastIncludedIndex),
			LastIncludedTerm:  int(req.Snapshot.LastIncludedTerm),
			StateMachine:      req.Snapshot.StateMachine,
		},
	})

	return &proto.InstallSnapshotResponse{
		Term: uint64(resp.Term),
	}, nil
}
