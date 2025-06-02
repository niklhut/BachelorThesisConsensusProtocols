package grpc

import (
	"context"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ServerIDInjectionInterceptor injects the peer ID into request metadata
type ServerIDInjectionInterceptor struct {
	PeerID int
}

// UnaryClientInterceptor returns a unary client interceptor that injects server ID
func (s *ServerIDInjectionInterceptor) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Inject peer ID into metadata
		md := metadata.New(map[string]string{
			"x-peer-id": strconv.Itoa(s.PeerID),
		})
		ctx = metadata.NewOutgoingContext(ctx, md)

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// StreamClientInterceptor returns a stream client interceptor that injects server ID
func (s *ServerIDInjectionInterceptor) StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		// Inject peer ID into metadata
		md := metadata.New(map[string]string{
			"x-peer-id": strconv.Itoa(s.PeerID),
		})
		ctx = metadata.NewOutgoingContext(ctx, md)

		return streamer(ctx, desc, cc, method, opts...)
	}
}
