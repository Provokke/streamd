package broker

import (
	"sort"
	"sync"
)

// Assignment is a set of partitions given to one group member. Gen increases
// with every rebalance of the group; consumers must ignore older generations.
type Assignment struct {
	Gen        uint64
	Partitions []int
}

type member struct {
	id     string
	seq    uint64
	notify func(Assignment)
}

type group struct {
	mu      sync.Mutex
	members []*member
	gen     uint64
	seq     uint64
	nparts  int
}

// JoinGroup adds a member to a consumer group for a topic. Partitions are
// spread across members (partition p goes to member p mod len(members), in
// join order). notify is called, possibly from another goroutine, with each
// member's new assignment after every join or leave — including this member's
// initial assignment — and may be called concurrently, so compare Gen.
// The returned func removes the member and triggers a rebalance.
func (b *Broker) JoinGroup(groupName string, t *Topic, memberID string, notify func(Assignment)) (leave func()) {
	key := groupName + "\x00" + t.Name
	b.mu.Lock() // released below, after the member is registered
	g := b.groups[key]
	if g == nil {
		g = &group{nparts: t.NumPartitions()}
		b.groups[key] = g
	}

	// Append while still holding b.mu so a concurrent last-leave can't delete
	// the group out from under us (lock order: b.mu then g.mu).
	g.mu.Lock()
	g.seq++
	m := &member{id: memberID, seq: g.seq, notify: notify}
	g.members = append(g.members, m)
	calls := g.rebalanceLocked()
	g.mu.Unlock()
	b.mu.Unlock()
	run(calls)

	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			for i, o := range g.members {
				if o == m {
					g.members = append(g.members[:i], g.members[i+1:]...)
					break
				}
			}
			calls := g.rebalanceLocked()
			empty := len(g.members) == 0
			g.mu.Unlock()
			run(calls)
			if empty {
				b.mu.Lock()
				g.mu.Lock()
				if len(g.members) == 0 && b.groups[key] == g {
					delete(b.groups, key)
				}
				g.mu.Unlock()
				b.mu.Unlock()
			}
		})
	}
}

func (g *group) rebalanceLocked() []func() {
	g.gen++
	if len(g.members) == 0 {
		return nil
	}
	parts := make([][]int, len(g.members))
	for p := 0; p < g.nparts; p++ {
		i := p % len(g.members)
		parts[i] = append(parts[i], p)
	}
	calls := make([]func(), 0, len(g.members))
	for i, m := range g.members {
		a := Assignment{Gen: g.gen, Partitions: parts[i]}
		sort.Ints(a.Partitions)
		n := m.notify
		calls = append(calls, func() { n(a) })
	}
	return calls
}

func run(calls []func()) {
	for _, c := range calls {
		c()
	}
}
