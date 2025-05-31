package util

import (
	"context"
	"sync"
)

type ReplicationTracker struct {
	majority     int
	successful   map[int]struct{}
	majorityDone []chan struct{}
	mu           sync.Mutex
}

func NewReplicationTracker(majority int) *ReplicationTracker {
	return &ReplicationTracker{
		majority:   majority,
		successful: make(map[int]struct{}),
	}
}

func (rt *ReplicationTracker) MarkSuccess(id int) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	if _, exists := rt.successful[id]; exists {
		return
	}
	rt.successful[id] = struct{}{}

	if len(rt.successful) >= rt.majority {
		for _, ch := range rt.majorityDone {
			close(ch)
		}
		rt.majorityDone = nil
	}
}

func (rt *ReplicationTracker) WaitForMajority(ctx context.Context) {
	rt.mu.Lock()

	if len(rt.successful) >= rt.majority {
		defer rt.mu.Unlock()
		return
	}

	ch := make(chan struct{})
	rt.majorityDone = append(rt.majorityDone, ch)
	rt.mu.Unlock()
	// Block until majority is reached or context is done
	select {
	case <-ctx.Done():
		return
	case <-ch:
	}
}

func (rt *ReplicationTracker) IsSuccessful(id int) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	_, exists := rt.successful[id]
	return exists
}

func (rt *ReplicationTracker) GetSuccessfulPeers() []int {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	var peers []int
	for id := range rt.successful {
		peers = append(peers, id)
	}
	return peers
}
