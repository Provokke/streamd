package client_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Provokke/streamd/broker"
	"github.com/Provokke/streamd/client"
	"github.com/Provokke/streamd/server"
)

func startServer(t *testing.T) string {
	t.Helper()
	b, err := broker.Open(broker.Config{Dir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	s := server.New(b, server.Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(ln)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		s.Shutdown(ctx)
		b.Close()
	})
	return ln.Addr().String()
}

// The connection drops after the subscription is registered but before its
// request is sent, while reconnect needs exclusive access to resubscribe. The
// client must recover (the Subscribe retries on the new connection) rather
// than deadlock holding the registration lock.
func TestSubscribeSurvivesDropBeforeRequest(t *testing.T) {
	addr := startServer(t)
	c, err := client.Dial(context.Background(), addr, client.WithReconnect(10*time.Millisecond), client.WithPingInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var fired atomic.Bool
	client.SetHookBeforeSubscribeCall(func() {
		if fired.CompareAndSwap(false, true) {
			c.KillConn()
			time.Sleep(50 * time.Millisecond) // let the read loop notice and start reconnecting
		}
	})
	defer client.SetHookBeforeSubscribeCall(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub, err := c.Subscribe(ctx, "t", client.SubscribeOptions{Group: "g"})
	if err != nil {
		t.Fatalf("Subscribe did not recover from a drop: %v", err)
	}
	defer sub.Close()
	if !fired.Load() {
		t.Fatal("hook never ran")
	}
	if c.Reconnects() < 1 {
		t.Fatalf("expected a reconnect, got %d", c.Reconnects())
	}
	if _, err := c.Publish(ctx, "t", "", []byte("still works")); err != nil {
		t.Fatal(err)
	}
}

func TestClosedSubscriptionNeverBlocksOnAbandonedChannel(t *testing.T) {
	addr := startServer(t)
	ctx := context.Background()
	c, _ := client.Dial(ctx, addr)
	defer c.Close()
	sub, _ := c.Subscribe(ctx, "t", client.SubscribeOptions{From: client.Earliest, Buffer: 1})
	for i := 0; i < 20; i++ {
		c.Publish(ctx, "t", "", []byte("x"))
	}
	time.Sleep(100 * time.Millisecond) // C is full; run() is parked on the send
	sub.Close()
	select {
	case _, ok := <-drain(sub.C):
		_ = ok
	case <-time.After(2 * time.Second):
		t.Fatal("C was never closed after Close(): run goroutine is stuck")
	}
}

// drain consumes ch until it closes, then signals.
func drain(ch <-chan client.Message) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()
	return done
}

func TestSlowConsumerQueueLimit(t *testing.T) {
	addr := startServer(t)
	ctx := context.Background()
	c, _ := client.Dial(ctx, addr)
	defer c.Close()
	sub, err := c.Subscribe(ctx, "t", client.SubscribeOptions{From: client.Earliest, Buffer: 1, MaxQueued: 50})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ { // never read sub.C
		c.Publish(ctx, "t", "", []byte("x"))
	}
	deadline := time.Now().Add(5 * time.Second)
	for !errors.Is(sub.Err(), client.ErrSlowConsumer) {
		if time.Now().After(deadline) {
			t.Fatalf("subscription not ended; err=%v", sub.Err())
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The connection itself is unaffected.
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestBinaryKeysAndValuesRoundTrip(t *testing.T) {
	addr := startServer(t)
	ctx := context.Background()
	c, _ := client.Dial(ctx, addr)
	defer c.Close()
	sub, _ := c.Subscribe(ctx, "bin", client.SubscribeOptions{From: client.Earliest})
	key := string([]byte{0xff, 0xfe, 'k', 0x00, 0x80})
	val := []byte{0x00, 0xff, 0xc3, 0x28, 0xa0, 0xa1}
	if _, err := c.Publish(ctx, "bin", key, val); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-sub.C:
		if m.Key != key || string(m.Value) != string(val) {
			t.Fatalf("lossy round trip: key %q value %q", m.Key, m.Value)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no message")
	}
}

func TestDialErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := client.Dial(ctx, "127.0.0.1:1"); err == nil {
		t.Fatal("expected connection refused")
	}
	addr := startServer(t)
	c, _ := client.Dial(context.Background(), addr)
	c.Close()
	if _, err := c.Publish(context.Background(), "t", "", nil); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("publish after close: %v", err)
	}
	if _, err := c.Subscribe(context.Background(), "t", client.SubscribeOptions{}); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("subscribe after close: %v", err)
	}
}

// Idempotent requests issued right as the connection dies must succeed once
// the client has reconnected, never surfacing ErrDisconnected.
func TestIdempotentCallsRetryAcrossReconnect(t *testing.T) {
	addr := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, addr, client.WithReconnect(5*time.Millisecond), client.WithPingInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < 40; i++ {
		c.KillConn() // no pause: the request races the client noticing the drop
		var err error
		switch i % 4 {
		case 0:
			err = c.Ping(ctx)
		case 1:
			_, err = c.Topics(ctx)
		case 2:
			_, err = c.Stats(ctx)
		case 3:
			err = c.CreateTopic(ctx, "idem", 2)
		}
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}
	if c.Reconnects() < 10 {
		t.Fatalf("test did not force reconnects: %d", c.Reconnects())
	}
}

// A request that passed the readiness check just before the connection died
// registers its reply slot while connLost is retiring the old connection. It
// must be failed (and retried after the reconnect), not parked forever on a
// socket nobody is reading.
func TestRequestRacingConnectionLossIsNotStranded(t *testing.T) {
	addr := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, addr, client.WithReconnect(10*time.Millisecond), client.WithPingInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	atCallOn := make(chan struct{})
	release := make(chan struct{})
	var once, once2 sync.Once
	client.SetHookBeforeCallOn(func() {
		once.Do(func() {
			close(atCallOn) // the request has seen "ready" but not yet registered
			<-release
		})
	})
	client.SetHookAfterPendingSwap(func() {
		once2.Do(func() {
			close(release)                     // let the request register now: pending map already swapped,
			time.Sleep(150 * time.Millisecond) // old socket not yet closed
		})
	})
	defer client.SetHookBeforeCallOn(nil)
	defer client.SetHookAfterPendingSwap(nil)

	errc := make(chan error, 1)
	go func() { errc <- c.Ping(ctx) }()
	<-atCallOn
	c.KillConn()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("Ping: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("request registered during connection loss was stranded")
	}
	if c.Reconnects() < 1 {
		t.Fatal("expected a reconnect")
	}
}
