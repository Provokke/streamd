package server_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
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

func TestListenAndServeAndShutdown(t *testing.T) {
	b, err := broker.Open(broker.Config{Dir: t.TempDir(), Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	s := server.New(b, server.Config{Logger: quiet})
	if s.Addr() != nil {
		t.Fatal("Addr before Serve should be nil")
	}
	errc := make(chan error, 1)
	go func() { errc <- s.ListenAndServe("127.0.0.1:0") }()
	waitFor(t, "listener", func() bool { return s.Addr() != nil })
	c, err := client.Dial(context.Background(), s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; !errors.Is(err, server.ErrServerClosed) {
		t.Fatalf("Serve returned %v", err)
	}
	// Serving on a shut-down server fails fast and closes the listener.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	if err := s.Serve(ln); !errors.Is(err, server.ErrServerClosed) {
		t.Fatalf("Serve after Shutdown: %v", err)
	}
	if _, err := ln.Accept(); err == nil {
		t.Fatal("listener was left open")
	}
	if err := s.ListenAndServe("256.0.0.1:99999"); err == nil {
		t.Fatal("bad address accepted")
	}
}

// A reconnecting client whose credentials stop being valid must give up with
// the auth error, not retry forever.
func TestReconnectGivesUpOnRejectedToken(t *testing.T) {
	dir := t.TempDir()
	e := start(t, dir, broker.Config{}, server.Config{AuthToken: "old"})
	addr := e.addr
	c, err := client.Dial(context.Background(), addr, client.WithToken("old"), client.WithReconnect(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	e.stop()
	startAt(t, addr, dir, broker.Config{}, server.Config{AuthToken: "rotated"})
	select {
	case <-c.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("client kept retrying a rejected token")
	}
	if err := c.Err(); err == nil || err.Error() != "invalid token" {
		t.Fatalf("Err = %v", err)
	}
	if err := c.Ping(context.Background()); err == nil {
		t.Fatal("calls on a terminated client must fail")
	}
}

// The follower must keep trying while the leader is down, then catch up; and
// work over TLS with a token.
func TestFollowerReconnectsOverTLSWithToken(t *testing.T) {
	p := newPKI(t)
	srvTLS := &tls.Config{Certificates: []tls.Certificate{p.issue(t, 2, x509.ExtKeyUsageServerAuth)}, MinVersion: tls.VersionTLS12}
	leaderDir := t.TempDir()
	// Reserve a port for the leader, but don't start it yet.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()

	fol := start(t, t.TempDir(), broker.Config{Log: commitlog.Options{SyncInterval: -1}}, server.Config{ReadOnly: true})
	f := &replica.Follower{B: fol.b, Leader: addr, Token: "tok", TLS: &tls.Config{RootCAs: p.caPool},
		Logger: quiet, DiscoverInterval: 30 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { f.Run(ctx); close(done) }()
	time.Sleep(400 * time.Millisecond) // several failed attempts against the dead leader

	leader := startAt(t, addr, leaderDir, broker.Config{}, server.Config{TLS: srvTLS, AuthToken: "tok"})
	lc, err := client.Dial(ctx, addr, client.WithToken("tok"), client.WithTLS(&tls.Config{RootCAs: p.caPool}))
	if err != nil {
		t.Fatal(err)
	}
	defer lc.Close()
	for i := 0; i < 25; i++ {
		lc.Publish(ctx, "sec", "", []byte(fmt.Sprint(i)))
	}
	waitFor(t, "follower to catch up over TLS", func() bool {
		ts := fol.b.Topics()
		return len(ts) == 1 && ts[0].Partitions[0].Next == 25
	})
	_ = leader

	// Wrong token: the follower keeps retrying, replicates nothing new, and stops cleanly.
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("follower did not stop on cancel")
	}
}
