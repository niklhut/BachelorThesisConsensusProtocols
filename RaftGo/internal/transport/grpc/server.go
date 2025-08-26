package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/niklhut/raft_go/internal/core/node"
	"github.com/niklhut/raft_go/internal/core/util"
	"github.com/niklhut/raft_go/internal/transport/grpc/proto"
	"google.golang.org/grpc"
)

type RaftGRPCServer struct {
	ownPeer        util.Peer
	peers          []util.Peer
	logger         *slog.Logger
	persistence    node.RaftNodePersistence
	collectMetrics bool
}

func NewRaftGRPCServer(
	ownPeer util.Peer,
	peers []util.Peer,
	persistence node.RaftNodePersistence,
	collectMetrics bool,
) *RaftGRPCServer {
	return &RaftGRPCServer{
		ownPeer:        ownPeer,
		peers:          peers,
		logger:         slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})),
		persistence:    persistence,
		collectMetrics: collectMetrics,
	}
}

func (s *RaftGRPCServer) OwnPeer() util.Peer {
	return s.ownPeer
}

func (s *RaftGRPCServer) Peers() []util.Peer {
	return s.peers
}

func (s *RaftGRPCServer) Serve(ctx context.Context) error {
	serverIDInterceptor := ServerIDInjectionInterceptor{PeerID: s.ownPeer.ID}

	// Build gRPC transport and RaftNode
	clientPool := NewGRPCClientPool(
		[]grpc.UnaryClientInterceptor{serverIDInterceptor.UnaryClientInterceptor()},
		[]grpc.StreamClientInterceptor{serverIDInterceptor.StreamClientInterceptor()},
	)
	transport := NewGRPCRaftNodeTransport(clientPool)

	config := util.NewRaftConfig()
	raftNode := node.NewRaftNode(s.ownPeer, s.peers, config, transport, s.persistence, s.collectMetrics)

	// Start server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.ownPeer.Port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	partitionInterceptor := NewNetworkPartitionInterceptor(s.logger)

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(partitionInterceptor.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(partitionInterceptor.StreamServerInterceptor()))

	// Register Services
	proto.RegisterRaftPeerServer(grpcServer, NewRaftPeerService(raftNode))
	proto.RegisterRaftClientServer(grpcServer, NewRaftClientService(raftNode))
	proto.RegisterPartitionServer(grpcServer, NewPartitionService(partitionInterceptor))

	s.logger.Info("Server listening", slog.Any("address", lis.Addr()))

	// Run server and node concurrently
	raftNode.Start()

	return grpcServer.Serve(lis)
}
