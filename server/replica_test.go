package server_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/Provokke/streamd/broker"
	"github.com/Provokke/streamd/client"
	"github.com/Provokke/streamd/commitlog"
	"github.com/Provokke/streamd/replica"
	"github.com/Provokke/streamd/server"
)

func runFollower(t *testing.T, leaderAddr string, fol *env, prune bool) (*replica.Follower, context.CancelFunc) {
	t.Helper()
	f := &replica.Follower{B: fol.b, Leader: leaderAddr, Logger: quiet, DiscoverInterval: 30 * time.Millisecond, Prune: prune}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return f, cancel
}

func topicNames(b *broker.Broker) []string {
	var out []string
	for _, ti := range b.Topics() {
		out = append(out, ti.Name)
	}
	return out
}

func TestFollowerMirrorsTopicDeletionAndRecreation(t *testing.T) {
	leader := start(t, t.TempDir(), broker.Config{Log: commitlog.Options{SyncInterval: -1}}, server.Config{})
	fol := start(t, t.TempDir(), broker.Config{Log: commitlog.Options{SyncInterval: -1}}, server.Config{ReadOnly: true})
	ctx := context.Background()
	lc := dial(t, leader.addr)
	lc.CreateTopic(ctx, "keep", 1)
	lc.CreateTopic(ctx, "doomed", 2)
	for i := 0; i < 6; i++ {
		lc.Publish(ctx, "keep", "", []byte("k"))
		lc.PublishTo(ctx, "doomed", i%2, "", []byte("d"))
	}
	runFollower(t, leader.addr, fol, false)
	waitFor(t, "both topics mirrored", func() bool {
		ts := fol.b.Topics()
		return len(ts) == 2 && ts[0].Name == "doomed" && ts[0].Partitions[0].Next == 3 && ts[1].Name == "keep" && ts[1].Partitions[0].Next == 6
	})

	if err := lc.DeleteTopic(ctx, "doomed"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "deletion to reach the follower", func() bool { return fmt.Sprint(topicNames(fol.b)) == "[keep]" })

	// Recreated with a different shape: the follower must follow the new one.
	lc.CreateTopic(ctx, "doomed", 3)
	for i := 0; i < 9; i++ {
		lc.PublishTo(ctx, "doomed", i%3, "", []byte("new"))
	}
	waitFor(t, "recreated topic mirrored with 3 partitions", func() bool {
		for _, ti := range fol.b.Topics() {
			if ti.Name == "doomed" && len(ti.Partitions) == 3 {
				return ti.Partitions[0].Next == 3 && ti.Partitions[1].Next == 3 && ti.Partitions[2].Next == 3
			}
		}
		return false
	})
	// "keep" was never disturbed.
	for _, ti := range fol.b.Topics() {
		if ti.Name == "keep" && ti.Partitions[0].Next != 6 {
			t.Fatalf("unrelated topic changed: %+v", ti)
		}
	}
}

func TestFollowerPrunesUnknownTopicsOnlyWhenAsked(t *testing.T) {
	for _, prune := range []bool{false, true} {
		t.Run(fmt.Sprint("prune=", prune), func(t *testing.T) {
			leader := start(t, t.TempDir(), broker.Config{}, server.Config{})
			fol := start(t, t.TempDir(), broker.Config{}, server.Config{ReadOnly: true})
			ctx := context.Background()
			fol.b.CreateTopic("stray", 1) // pre-existing local data the leader has never heard of
			fol.b.Publish("stray", -1, nil, []byte("precious"))
			dial(t, leader.addr).Publish(ctx, "real", "", []byte("x"))
			runFollower(t, leader.addr, fol, prune)
			waitFor(t, "real topic mirrored", func() bool {
				for _, n := range topicNames(fol.b) {
					if n == "real" {
						return true
					}
				}
				return false
			})
			time.Sleep(200 * time.Millisecond)
			hasStray := false
			for _, n := range topicNames(fol.b) {
				hasStray = hasStray || n == "stray"
			}
			if prune && hasStray {
				t.Fatal("-follow-prune should have removed the unknown topic")
			}
			if !prune && !hasStray {
				t.Fatal("a topic the leader never had was deleted without Prune: pointing at the wrong leader would wipe data")
			}
		})
	}
}

// Group positions follow the data, so promoting a follower (restarting it as a
// leader) lets consumers resume where they were.
func TestFollowerReplicatesGroupOffsetsAndCanBePromoted(t *testing.T) {
	leader := start(t, t.TempDir(), broker.Config{}, server.Config{})
	folDir := t.TempDir()
	fol := start(t, folDir, broker.Config{}, server.Config{ReadOnly: true})
	ctx := context.Background()
	lc := dial(t, leader.addr)
	lc.CreateTopic(ctx, "pr", 2)
	for i := 0; i < 20; i++ {
		lc.PublishTo(ctx, "pr", i%2, "", []byte(fmt.Sprint(i)))
	}
	sub, _ := lc.Subscribe(ctx, "pr", client.SubscribeOptions{Group: "g", From: client.Earliest})
	var last [2]client.Message
	for i := 0; i < 12; i++ { // consume and ack 6 per partition
		m := recv(t, sub)
		last[m.Partition] = m
	}
	sub.Ack(last[0])
	sub.Ack(last[1])
	runFollower(t, leader.addr, fol, false)
	waitFor(t, "data and offsets replicated", func() bool {
		ts := fol.b.Topics()
		if len(ts) != 1 || ts[0].Partitions[0].Next != 10 || ts[0].Partitions[1].Next != 10 {
			return false
		}
		a, _ := fol.b.Offsets().Get("g", "pr", 0)
		b, _ := fol.b.Offsets().Get("g", "pr", 1)
		return a == last[0].Offset+1 && b == last[1].Offset+1
	})

	// Promote: stop replicating, stop the read-only server, serve the same data writable.
	fol.s.Shutdown(ctx)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	promoted := server.New(fol.b, server.Config{Logger: quiet})
	go promoted.Serve(ln)
	t.Cleanup(func() { promoted.Shutdown(ctx) })

	pc := dial(t, ln.Addr().String())
	if _, err := pc.Publish(ctx, "pr", "", []byte("after-promotion")); err != nil {
		t.Fatalf("promoted node should accept writes: %v", err)
	}
	g, err := pc.Subscribe(ctx, "pr", client.SubscribeOptions{Group: "g"})
	if err != nil {
		t.Fatal(err)
	}
	// The two partitions are consumed concurrently, so how the first 12 messages
	// split between them varies. Whatever the split, the promoted node must hand
	// each partition back starting exactly after that partition's last ack, and
	// contiguously: nothing skipped, nothing replayed.
	remaining := 0
	for p := 0; p < 2; p++ {
		remaining += 10 - int(last[p].Offset+1)
	}
	got := map[int][]int64{}
	for i := 0; i < remaining+1; i++ { // everything unconsumed + the post-promotion message
		m := recv(t, g)
		got[m.Partition] = append(got[m.Partition], m.Offset)
	}
	for p := 0; p < 2; p++ {
		for i, off := range got[p] {
			if want := last[p].Offset + 1 + int64(i); off != want {
				t.Fatalf("partition %d: delivery %d is offset %d, want %d (all: %v)", p, i, off, want, got)
			}
		}
	}
}
