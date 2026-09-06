package server

import (
	"sync"
	"testing"

	"github.com/Provokke/streamd/broker"
)

// Notifications for one member can be delivered out of order (they run on the
// goroutines that triggered each rebalance). The mailbox must keep the newest
// generation no matter the order.
func TestMailboxKeepsNewestGeneration(t *testing.T) {
	orders := [][]uint64{{1, 2, 3}, {3, 2, 1}, {2, 3, 1}, {2, 1, 3}, {3, 1, 2}, {1, 3, 2}}
	for _, order := range orders {
		sub := &subscription{wake: make(chan struct{}, 1)}
		for _, g := range order {
			sub.post(broker.Assignment{Gen: g, Partitions: []int{int(g)}})
		}
		if sub.pending == nil || sub.pending.Gen != 3 || sub.pending.Partitions[0] != 3 {
			t.Fatalf("order %v: pending = %+v", order, sub.pending)
		}
		select {
		case <-sub.wake:
		default:
			t.Fatalf("order %v: applier was never woken", order)
		}
	}
}

func TestMailboxConcurrentPostsNeverBlockOrRegress(t *testing.T) {
	sub := &subscription{wake: make(chan struct{}, 1)}
	var wg sync.WaitGroup
	for g := uint64(1); g <= 200; g++ {
		wg.Add(1)
		go func(g uint64) {
			defer wg.Done()
			sub.post(broker.Assignment{Gen: g})
		}(g)
	}
	wg.Wait() // would hang if post could block on a full wake channel
	if sub.pending.Gen != 200 {
		t.Fatalf("pending gen %d, want 200", sub.pending.Gen)
	}
}
