package grpc

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/niklhut/raft_go/internal/core/node"
	"github.com/niklhut/raft_go/internal/core/util"
	"github.com/niklhut/raft_go/internal/transport/grpc/proto"
	"google.golang.org/grpc"
)

type RaftGRPCServer struct {
	ownPeer util.Peer
	peers   []util.Peer
}

func NewRaftGRPCServer(ownPeer util.Peer, peers []util.Peer) *RaftGRPCServer {
	return &RaftGRPCServer{
		ownPeer: ownPeer,
		peers:   peers,
	}
}

func (s *RaftGRPCServer) OwnPeer() util.Peer {
	return s.ownPeer
}

func (s *RaftGRPCServer) Peers() []util.Peer {
	return s.peers
}

func (s *RaftGRPCServer) Serve(ctx context.Context) error {
	// Build gRPC transport and RaftNode
	clientPool := NewGRPCClientPool()
	transport := NewGRPCRaftPeerTransport(clientPool)

	config := util.NewRaftConfig()
	raftNode := node.NewRaftNode(s.ownPeer, s.peers, config, transport)

	// Start server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.ownPeer.Port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	grpcServer := grpc.NewServer()

	// Register RaftPeerService
	proto.RegisterRaftPeerServer(grpcServer, NewRaftPeerService(raftNode))

	log.Printf("Server listening on %v", lis.Addr())

	// Run server and node concurrently
	raftNode.Start()

	return grpcServer.Serve(lis)
}
