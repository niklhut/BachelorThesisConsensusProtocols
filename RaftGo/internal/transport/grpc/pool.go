package grpc

import (
	"fmt"
	"sync"

	"github.com/niklhut/raft_go/internal/core/util"
	"github.com/niklhut/raft_go/internal/transport/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCClientPool struct {
	mu      sync.Mutex
	clients map[int]*grpc.ClientConn
}

func NewGRPCClientPool() *GRPCClientPool {
	return &GRPCClientPool{
		clients: make(map[int]*grpc.ClientConn),
	}
}

func (p *GRPCClientPool) GetClient(to util.Peer) (proto.RaftPeerClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn, ok := p.clients[to.ID]
	if !ok {
		var err error
		conn, err = grpc.NewClient(
			fmt.Sprintf("%s:%d", to.Address, to.Port),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, err
		}
		p.clients[to.ID] = conn
	}

	return proto.NewRaftPeerClient(conn), nil
}

func (p *GRPCClientPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, conn := range p.clients {
		conn.Close()
	}
}
