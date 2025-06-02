package grpc

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/niklhut/raft_go/internal/transport/grpc/proto"

	"github.com/niklhut/raft_go/internal/core/node"
	"github.com/niklhut/raft_go/internal/core/util"
)

type RaftClientService struct {
	proto.UnimplementedRaftClientServer
	node *node.RaftNode
}

func NewRaftClientService(node *node.RaftNode) *RaftClientService {
	return &RaftClientService{node: node}
}

func (s *RaftClientService) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	response, err := s.node.Put(ctx, util.PutRequest{
		Key:   req.Key,
		Value: req.Value,
	})

	if err != nil {
		return nil, err
	}

	return &proto.PutResponse{
		Success:    response.Success,
		LeaderHint: convertPeerToProto(response.LeaderHint),
	}, nil
}

func (s *RaftClientService) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	response := s.node.Get(ctx, util.GetRequest{
		Key: req.Key,
	})

	return &proto.GetResponse{
		Value:      response.Value,
		LeaderHint: convertPeerToProto(response.LeaderHint),
	}, nil
}

func (s *RaftClientService) GetDebug(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	response := s.node.GetDebug(ctx, util.GetRequest{
		Key: req.Key,
	})

	return &proto.GetResponse{
		Value: response.Value,
	}, nil
}

func (s *RaftClientService) GetServerState(ctx context.Context, _ *emptypb.Empty) (*proto.ServerStateResponse, error) {
	state := s.node.GetState(ctx)
	return &proto.ServerStateResponse{
		Id:    uint32(state.ID),
		State: convertServerStateToProto(state.State),
	}, nil
}

func (s *RaftClientService) GetServerTerm(ctx context.Context, _ *emptypb.Empty) (*proto.ServerTermResponse, error) {
	term := s.node.GetTerm(ctx)
	return &proto.ServerTermResponse{
		Id:   uint32(term.ID),
		Term: uint64(term.Term),
	}, nil
}

func convertPeerToProto(peer *util.Peer) *proto.Peer {
	if peer == nil {
		return nil
	}
	return &proto.Peer{
		Id:      uint32(peer.ID),
		Address: peer.Address,
		Port:    uint32(peer.Port),
	}
}

func convertServerStateToProto(state util.ServerState) proto.ServerState {
	switch state {
	case util.ServerStateLeader:
		return proto.ServerState_LEADER
	case util.ServerStateFollower:
		return proto.ServerState_FOLLOWER
	case util.ServerStateCandidate:
		return proto.ServerState_CANDIDATE
	default:
		return proto.ServerState_CANDIDATE
	}
}
