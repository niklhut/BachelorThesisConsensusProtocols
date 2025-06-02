package grpc

import (
	"context"

	"github.com/niklhut/raft_go/internal/transport/grpc/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type PartitionService struct {
	proto.UnimplementedPartitionServer
	partitioner *NetworkPartitionInterceptor
}

func NewPartitionService(partitioner *NetworkPartitionInterceptor) *PartitionService {
	return &PartitionService{partitioner: partitioner}
}

func (s *PartitionService) BlockPeers(ctx context.Context, req *proto.BlockPeersRequest) (*emptypb.Empty, error) {
	peerIds := make([]int, len(req.PeerIds))
	for i, id := range req.PeerIds {
		peerIds[i] = int(id)
	}

	s.partitioner.BlockPeers(peerIds)
	return &emptypb.Empty{}, nil
}

func (s *PartitionService) ClearBlockedPeers(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	s.partitioner.ClearBlockedPeers()
	return &emptypb.Empty{}, nil
}
