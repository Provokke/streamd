package server_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Provokke/streamd/broker"
	"github.com/Provokke/streamd/client"
	"github.com/Provokke/streamd/commitlog"
	"github.com/Provokke/streamd/server"
)

// proxy is a TCP forwarder whose connections can be severed on demand, to
// simulate network failures while the server keeps running.
type proxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	conns  []net.Conn
	down   bool
}

func newProxy(t testing.TB, target string) *proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, target: target}
	t.Cleanup(func() { ln.Close(); p.sever() })
	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			down := p.down
			p.mu.Unlock()
			if down {
				in.Close()
				continue
			}
			out, err := net.Dial("tcp", p.target)
			if err != nil {
				in.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, in, out)
			p.mu.Unlock()
			go func() { copyClose(in, out) }()
			go func() { copyClose(out, in) }()
		}
	}()
	return p
}

func copyClose(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	dst.Close()
	src.Close()
}

func (p *proxy) addr() string { return p.ln.Addr().String() }

// cut drops every live connection and refuses new ones until heal().
func (p *proxy) cut() {
	p.mu.Lock()
	p.down = true
	p.mu.Unlock()
	p.sever()
}

func (p *proxy) heal() {
	p.mu.Lock()
	p.down = false
	p.mu.Unlock()
}

func (p *proxy) sever() {
	p.mu.Lock()
	cs := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, c := range cs {
		c.Close()
	}
}

// Messages published to partitions that had not yet delivered anything when
// the connection dropped must not be skipped by the resumed subscription.
func TestReconnectDoesNotSkipIdlePartitions(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{})
	px := newProxy(t, e.addr)
	ctx := context.Background()
	direct := dial(t, e.addr)
	direct.CreateTopic(ctx, "idle", 3)

	c, err := client.Dial(ctx, px.addr(), client.WithReconnect(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	sub, err := c.Subscribe(ctx, "idle", client.SubscribeOptions{}) // Latest on all partitions
	if err != nil {
		t.Fatal(err)
	}
	direct.PublishTo(ctx, "idle", 0, "", []byte("p0-before"))
	if m := recv(t, sub); string(m.Value) != "p0-before" {
		t.Fatalf("got %+v", m)
	}

	px.cut()
	waitFor(t, "server to notice the cut", func() bool { return e.s.Stats()["subscriptions"] == 0 })
	// Published during the outage, to partitions that never delivered anything.
	direct.PublishTo(ctx, "idle", 1, "", []byte("p1-outage"))
	direct.PublishTo(ctx, "idle", 2, "", []byte("p2-outage"))
	direct.PublishTo(ctx, "idle", 0, "", []byte("p0-outage"))
	px.heal()

	got := map[string]bool{}
	for i := 0; i < 3; i++ {
		got[string(recv(t, sub).Value)] = true
	}
	for _, w := range []string{"p0-outage", "p1-outage", "p2-outage"} {
		if !got[w] {
			t.Fatalf("lost %s across reconnect; got %v", w, got)
		}
	}
	// The counter ticks just after the resubscribe completes, which can be
	// after the first replayed message is already visible.
	waitFor(t, "reconnect to be counted", func() bool { return c.Reconnects() == 1 })
}

// Same hole for groups with no committed offset yet: "latest" must be pinned
// when first resolved, not re-resolved after a reconnect.
func TestGroupLatestPinnedAcrossReconnect(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{})
	px := newProxy(t, e.addr)
	ctx := context.Background()
	direct := dial(t, e.addr)
	direct.CreateTopic(ctx, "pin", 2)
	direct.PublishTo(ctx, "pin", 0, "", []byte("old"))

	c, _ := client.Dial(ctx, px.addr(), client.WithReconnect(100*time.Millisecond))
	defer c.Close()
	sub, err := c.Subscribe(ctx, "pin", client.SubscribeOptions{Group: "pg", From: client.Latest})
	if err != nil {
		t.Fatal(err)
	}
	waitAssign(t, sub, 2)
	px.cut()
	waitFor(t, "disconnect", func() bool { return e.s.Stats()["subscriptions"] == 0 })
	direct.PublishTo(ctx, "pin", 0, "", []byte("during-0"))
	direct.PublishTo(ctx, "pin", 1, "", []byte("during-1"))
	px.heal()
	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case m := <-sub.C:
			got[string(m.Value)] = true
		case <-time.After(3 * time.Second):
			t.Fatalf("got only %v; assignment=%v reconnects=%d offsets=%v", got, sub.Assignment(), c.Reconnects(), e.b.Offsets().Snapshot())
		}
	}
	if !got["during-0"] || !got["during-1"] || got["old"] {
		t.Fatalf("got %v", got)
	}
}

// A dropped connection with a pending Subscribe must neither deadlock the
// client nor leave subscriptions behind on the server.
func TestSubscribeDuringFlappingConnection(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{DefaultPartitions: 2}, server.Config{})
	px := newProxy(t, e.addr)
	c, err := client.Dial(context.Background(), px.addr(), client.WithReconnect(5*time.Millisecond), client.WithPingInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	stop := make(chan struct{})
	var flaps sync.WaitGroup
	flaps.Add(1)
	go func() {
		defer flaps.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(1+rand.Intn(6)) * time.Millisecond):
				px.cut()
				time.Sleep(time.Duration(rand.Intn(4)) * time.Millisecond)
				px.heal()
			}
		}
	}()

	var wg sync.WaitGroup
	var ok, failed int64
	var cmu sync.Mutex
	endAt := time.Now().Add(2 * time.Second)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; time.Now().Before(endAt); i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				s, err := c.Subscribe(ctx, "flap", client.SubscribeOptions{Group: fmt.Sprintf("g%d", w)})
				cmu.Lock()
				if err != nil {
					failed++
				} else {
					ok++
				}
				cmu.Unlock()
				cancel()
				if s != nil {
					s.Close()
				}
			}
		}(w)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("Subscribe deadlocked while the connection flapped")
	}
	close(stop)
	flaps.Wait()
	px.heal()
	t.Logf("subscribes ok=%d failed=%d reconnects=%d", ok, failed, c.Reconnects())
	if ok == 0 || c.Reconnects() < 3 {
		t.Fatalf("test did not exercise reconnects (ok=%d reconnects=%d)", ok, c.Reconnects())
	}
	// The client is still healthy and the server holds no stale subscriptions.
	if err := c.Ping(withTimeout(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "server subscriptions to drain", func() bool { return e.s.Stats()["subscriptions"] == 0 })
}

func withTimeout(t testing.TB, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// When retention overtakes a windowed subscriber, the credits of the skipped
// in-flight messages must be returned or the subscription eventually stalls.
func TestRetentionSkipDoesNotLeakCredits(t *testing.T) {
	e := start(t, t.TempDir(),
		broker.Config{Log: commitlog.Options{SegmentBytes: 150, RetentionBytes: 300, SyncInterval: -1}},
		server.Config{})
	ctx := context.Background()
	pc := dial(t, e.addr)
	c := dial(t, e.addr)
	sub, err := c.Subscribe(ctx, "skip", client.SubscribeOptions{From: client.Earliest, Window: 2})
	if err != nil {
		t.Fatal(err)
	}
	var lastOff int64 = -1
	// Each round: take the in-flight window without acking, let retention
	// overtake the subscriber, then drain by acking.
	for round := 0; round < 6; round++ {
		for i := 0; i < 40; i++ {
			pc.Publish(ctx, "skip", "", []byte(fmt.Sprintf("r%d-%02d", round, i)))
		}
		e.b.EnforceRetention()
		deadline := time.After(10 * time.Second)
		for {
			select {
			case m := <-sub.C:
				lastOff = m.Offset
				sub.Ack(m)
			case <-deadline:
				t.Fatalf("round %d: subscription stalled at offset %d (credits leaked)", round, lastOff)
			}
			next := e.b.Topics()[0].Partitions[0].Next
			if lastOff >= next-1 {
				break
			}
		}
	}
}

// If the server no longer has a subscribed topic when the client reconnects,
// that one subscription ends with the server's error; the connection must
// still come up and keep serving everything else.
func TestReconnectSurvivesUnresubscribableSubscription(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{})
	addr := e.addr
	ctx := context.Background()
	c, err := client.Dial(ctx, addr, client.WithReconnect(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.Publish(ctx, "gone", "", []byte("x"))
	doomed, err := c.Subscribe(ctx, "gone", client.SubscribeOptions{From: client.Earliest})
	if err != nil {
		t.Fatal(err)
	}
	recv(t, doomed)
	e.stop()
	// Same address, empty data directory, topics may not be auto-created.
	startAt(t, addr, t.TempDir(), broker.Config{DisableAutoCreate: true}, server.Config{})

	select {
	case _, ok := <-doomed.C:
		if ok {
			t.Fatal("unexpected message")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("reconnect wedged on a subscription the server refuses")
	}
	if err := doomed.Err(); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("subscription error = %v", err)
	}
	var se client.ServerError
	if !errors.As(doomed.Err(), &se) {
		t.Fatalf("expected a ServerError, got %T", doomed.Err())
	}
	// The connection itself is fully usable.
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.CreateTopic(cctx, "fresh", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Publish(cctx, "fresh", "", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if c.Err() != nil {
		t.Fatalf("client died: %v", c.Err())
	}
}

// An ack for a message delivered before a reconnect must not be sent: the
// server has since redelivered those offsets and a late cumulative ack would
// commit past messages the consumer has not seen on the new connection.
func TestStaleAckAfterReconnectIsDropped(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{})
	px := newProxy(t, e.addr)
	ctx := context.Background()
	direct := dial(t, e.addr)
	for i := 0; i < 6; i++ {
		direct.Publish(ctx, "sa", "", []byte(fmt.Sprint(i)))
	}
	c, _ := client.Dial(ctx, px.addr(), client.WithReconnect(30*time.Millisecond))
	defer c.Close()
	sub, err := c.Subscribe(ctx, "sa", client.SubscribeOptions{Group: "g", From: client.Earliest, Window: 3})
	if err != nil {
		t.Fatal(err)
	}
	old := []client.Message{recv(t, sub), recv(t, sub), recv(t, sub)} // 0,1,2 — window full, unacked

	px.cut()
	waitFor(t, "disconnect", func() bool { return e.s.Stats()["subscriptions"] == 0 })
	px.heal()
	waitFor(t, "reconnect", func() bool { return c.Reconnects() == 1 })

	if err := sub.Ack(old[2]); !errors.Is(err, client.ErrStaleAck) {
		t.Fatalf("stale ack: got %v, want ErrStaleAck", err)
	}
	time.Sleep(100 * time.Millisecond)
	if v, _ := e.b.Offsets().Get("g", "sa", 0); v != 0 {
		t.Fatalf("stale ack moved the committed offset to %d", v)
	}
	// The same offsets come again on the new connection and acking those works.
	for i := 0; i < 3; i++ {
		m := recv(t, sub)
		if m.Offset != int64(i) {
			t.Fatalf("redelivery %d: got offset %d", i, m.Offset)
		}
		if i == 2 {
			if err := sub.Ack(m); err != nil {
				t.Fatalf("fresh ack: %v", err)
			}
		}
	}
	waitFor(t, "fresh ack to commit", func() bool { v, _ := e.b.Offsets().Get("g", "sa", 0); return v == 3 })
}
