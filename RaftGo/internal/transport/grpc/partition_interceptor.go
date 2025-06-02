package grpc

import (
	"context"
	"log/slog"
	"strconv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// NetworkPartitionInterceptor blocks requests from certain peers
type NetworkPartitionInterceptor struct {
	mu           sync.RWMutex
	blockedPeers map[int]struct{}
	logger       *slog.Logger
}

func NewNetworkPartitionInterceptor(logger *slog.Logger) *NetworkPartitionInterceptor {
	return &NetworkPartitionInterceptor{
		blockedPeers: make(map[int]struct{}),
		logger:       logger,
	}
}

// UnaryServerInterceptor returns a unary server interceptor that blocks requests from blocked peers
func (n *NetworkPartitionInterceptor) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := n.checkBlocked(ctx); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a stream server interceptor that blocks requests from blocked peers
func (n *NetworkPartitionInterceptor) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := n.checkBlocked(ss.Context()); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// checkBlocked checks if the request should be blocked based on peer ID in metadata
func (n *NetworkPartitionInterceptor) checkBlocked(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		// No metadata, allow the request
		return nil
	}

	peerIDs := md.Get("x-peer-id")
	if len(peerIDs) == 0 {
		// No peer ID in metadata, allow the request
		return nil
	}

	// Use the first peer ID found
	peerIDStr := peerIDs[0]
	peerID, err := strconv.Atoi(peerIDStr)
	if err != nil {
		// Invalid peer ID format, log and allow the request
		n.logger.Warn("Invalid peer ID format", "peer_id", peerIDStr, "error", err)
		return nil
	}

	n.mu.RLock()
	_, blocked := n.blockedPeers[peerID]
	n.mu.RUnlock()

	if blocked {
		n.logger.Debug("Blocking request from peer", "peer_id", peerID)
		return status.Error(codes.Unavailable, "Request blocked by network partition")
	}

	return nil
}

func (n *NetworkPartitionInterceptor) BlockPeers(ids []int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, id := range ids {
		n.blockedPeers[id] = struct{}{}
	}

	n.logger.Info("Blocked peers", slog.Any("peers", n.blockedPeers))
}

func (n *NetworkPartitionInterceptor) ClearBlockedPeers() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.blockedPeers = make(map[int]struct{})
	n.logger.Info("Cleared blocked peers")
}
