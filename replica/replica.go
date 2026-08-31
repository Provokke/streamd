// Package replica implements asynchronous leader->follower replication.
//
// A Follower connects to a leader as an ordinary client, discovers topics,
// and streams every record into the local broker's logs preserving offsets
// and timestamps. Run the follower's server with ReadOnly so it only serves
// reads. Replication is asynchronous: a follower may lag, and there is no
// automatic failover or leader election.
package replica

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/Provokke/streamd/broker"
	"github.com/Provokke/streamd/client"
	"github.com/Provokke/streamd/commitlog"
)

// Follower mirrors a leader into a local broker.
type Follower struct {
	B      *broker.Broker
	Leader string
	Token  string
	TLS    *tls.Config // if set, connect to the leader over TLS
	Logger *slog.Logger
	// DiscoverInterval is how often the leader is polled for topic changes and
	// consumer-group offsets (default 2s).
	DiscoverInterval time.Duration
	// Prune also deletes local topics that the leader does not have at all, even
	// ones this process never saw on it. Off by default so that pointing a
	// follower at the wrong (empty) leader cannot wipe its data. Topics that
	// were seen on the leader and later deleted there are always deleted locally.
	Prune bool

	mu   sync.Mutex
	seen map[string]bool // topics observed on the leader by this process
}

// Run blocks until ctx is cancelled, reconnecting with backoff on failure.
func (f *Follower) Run(ctx context.Context) error {
	if f.Logger == nil {
		f.Logger = slog.Default()
	}
	if f.DiscoverInterval <= 0 {
		f.DiscoverInterval = 2 * time.Second
	}
	backoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		start := time.Now()
		err := f.session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Since(start) > 10*time.Second {
			backoff = 250 * time.Millisecond
		}
		f.Logger.Warn("replication session ended; retrying", "leader", f.Leader, "err", err, "in", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		if backoff *= 2; backoff > 10*time.Second {
			backoff = 10 * time.Second
		}
	}
	return ctx.Err()
}

// followed is one topic being mirrored in the current session.
type followed struct {
	cancel context.CancelFunc
	done   chan struct{}
	parts  int
}

func (f *Follower) session(ctx context.Context) error {
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	opts := []client.Option{client.WithToken(f.Token)}
	if f.TLS != nil {
		opts = append(opts, client.WithTLS(f.TLS))
	}
	c, err := client.Dial(dctx, f.Leader, opts...)
	cancel()
	if err != nil {
		return err
	}
	defer c.Close()
	f.Logger.Info("replicating from leader", "leader", f.Leader)

	sctx, stop := context.WithCancel(ctx)
	defer stop()
	errc := make(chan error, 1)
	following := map[string]*followed{}
	defer func() {
		stop()
		for _, fl := range following {
			<-fl.done
		}
	}()
	stopFollowing := func(name string) {
		if fl := following[name]; fl != nil {
			fl.cancel()
			<-fl.done
			delete(following, name)
		}
	}

	tick := time.NewTicker(f.DiscoverInterval)
	defer tick.Stop()
	for {
		rctx, rcancel := context.WithTimeout(sctx, 5*time.Second)
		topics, err := c.Topics(rctx)
		rcancel()
		if err != nil {
			return err
		}
		f.syncTopics(topics, following, stopFollowing)
		f.syncGroupOffsets(sctx, c)
		for _, ti := range topics {
			if following[ti.Name] != nil {
				continue
			}
			tctx, tcancel := context.WithCancel(sctx)
			fl := &followed{cancel: tcancel, done: make(chan struct{}), parts: len(ti.Partitions)}
			following[ti.Name] = fl
			go func() {
				defer close(fl.done)
				if err := f.follow(tctx, c, ti); err != nil && tctx.Err() == nil {
					select {
					case errc <- err:
					default:
					}
				}
			}()
		}
		select {
		case <-tick.C:
		case err := <-errc:
			return err
		case <-c.Done():
			return c.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// syncTopics reconciles local topics with the leader's list: topics deleted on
// the leader are deleted here, and topics recreated with a different partition
// count are dropped so they can be mirrored afresh.
func (f *Follower) syncTopics(leader []client.TopicInfo, following map[string]*followed, stopFollowing func(string)) {
	f.mu.Lock()
	if f.seen == nil {
		f.seen = map[string]bool{}
	}
	present := make(map[string]int, len(leader))
	for _, ti := range leader {
		present[ti.Name] = len(ti.Partitions)
		f.seen[ti.Name] = true
	}
	seen := make(map[string]bool, len(f.seen))
	for n := range f.seen {
		seen[n] = true
	}
	f.mu.Unlock()

	for _, local := range f.B.Topics() {
		n, onLeader := present[local.Name]
		switch {
		case onLeader && n == len(local.Partitions):
			continue
		case onLeader: // same name, different shape: a recreated topic
			f.Logger.Warn("leader topic has a different partition count; recreating locally",
				"topic", local.Name, "local", len(local.Partitions), "leader", n)
		case seen[local.Name] || f.Prune:
			f.Logger.Info("topic deleted on leader; deleting locally", "topic", local.Name)
		default:
			continue // never seen on this leader: leave it alone
		}
		stopFollowing(local.Name)
		if err := f.B.DeleteTopic(local.Name); err != nil && !errors.Is(err, broker.ErrNoTopic) {
			f.Logger.Error("delete local topic", "topic", local.Name, "err", err)
		}
		if !onLeader {
			f.mu.Lock()
			delete(f.seen, local.Name)
			f.mu.Unlock()
		}
	}
	// A followed topic that vanished before it was ever created locally.
	for name := range following {
		if _, ok := present[name]; !ok {
			stopFollowing(name)
		}
	}
}

// syncGroupOffsets copies the leader's committed consumer-group positions so a
// promoted follower resumes each group where the leader left it.
func (f *Follower) syncGroupOffsets(ctx context.Context, c *client.Client) {
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	groups, err := c.Groups(rctx, "", "")
	if err != nil {
		f.Logger.Debug("group offset sync failed", "err", err)
		return
	}
	for _, g := range groups {
		if t, err := f.B.Topic(g.Topic, false); err == nil && g.Partition < t.NumPartitions() {
			f.B.CommitIfCurrent(g.Group, t, g.Partition, g.Committed)
		}
	}
}

// follow mirrors every partition of one topic.
func (f *Follower) follow(ctx context.Context, c *client.Client, ti client.TopicInfo) error {
	t, err := f.B.CreateTopic(ti.Name, len(ti.Partitions))
	if errors.Is(err, broker.ErrExists) {
		// A local topic with another partition count cannot be the leader's.
		if derr := f.B.DeleteTopic(ti.Name); derr != nil {
			return derr
		}
		t, err = f.B.CreateTopic(ti.Name, len(ti.Partitions))
	}
	if err != nil {
		return err
	}
	g, gctx := errgroup(ctx)
	for _, pi := range ti.Partitions {
		g.go_(func() error { return f.followPartition(gctx, c, t, pi) })
	}
	return g.wait()
}

func (f *Follower) followPartition(ctx context.Context, c *client.Client, t *broker.Topic, pi client.PartitionInfo) error {
	lg := t.Parts[pi.Partition]
	// Reconcile ranges: if the local log cannot be extended contiguously from
	// the leader (we fell behind retention, or the leader lost data), restart
	// the local log at the leader's earliest retained offset.
	if next := lg.Next(); next < pi.Earliest || next > pi.Next {
		f.Logger.Warn("resetting local partition to leader range", "topic", t.Name, "partition", pi.Partition,
			"local_next", next, "leader_earliest", pi.Earliest, "leader_next", pi.Next)
		if err := lg.Reset(pi.Earliest); err != nil {
			return err
		}
	}
	sub, err := c.Subscribe(ctx, t.Name, client.SubscribeOptions{
		From: client.At(lg.Next()), Partition: client.Partition(pi.Partition), Buffer: 256})
	if err != nil {
		return err
	}
	defer sub.Close()
	for {
		select {
		case m, ok := <-sub.C:
			if !ok {
				if e := sub.Err(); e != nil {
					return e
				}
				return errors.New("subscription closed")
			}
			rec := commitlog.Record{Offset: m.Offset, Timestamp: m.Timestamp.UnixMilli(),
				Key: []byte(m.Key), Value: m.Value}
			if err := lg.Replicate([]commitlog.Record{rec}); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// group runs funcs concurrently; the first error cancels the rest.
type group struct {
	wg     sync.WaitGroup
	once   sync.Once
	err    error
	cancel context.CancelFunc
}

func errgroup(ctx context.Context) (*group, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &group{cancel: cancel}, ctx
}

func (g *group) go_(fn func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if err := fn(); err != nil {
			g.once.Do(func() { g.err = err; g.cancel() })
		}
	}()
}

func (g *group) wait() error {
	g.wg.Wait()
	g.cancel()
	return g.err
}
