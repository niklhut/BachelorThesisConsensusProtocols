package node

import (
	"context"

	"github.com/niklhut/raft_go/internal/core/util"
)

type RaftNodeApplication interface {
	OwnPeer() util.Peer
	Peers() []util.Peer
	Serve(ctx context.Context) error
}
