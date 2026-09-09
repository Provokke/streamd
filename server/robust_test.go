package server_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Provokke/streamd/broker"
	"github.com/Provokke/streamd/client"
	"github.com/Provokke/streamd/commitlog"
	"github.com/Provokke/streamd/protocol"
	"github.com/Provokke/streamd/replica"
	"github.com/Provokke/streamd/server"
)

// raw is a bare protocol connection for abuse testing.
type raw struct {
	t  testing.TB
	nc net.Conn
	r  *bufio.Reader
}

func rawDial(t testing.TB, addr string) *raw {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	return &raw{t: t, nc: nc, r: bufio.NewReader(nc)}
}

func (c *raw) send(line string) {
	c.t.Helper()
	c.nc.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := fmt.Fprintln(c.nc, line); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

// recv returns the next frame, or nil if the server closed the connection.
func (c *raw) recv() *protocol.Frame {
	c.t.Helper()
	c.nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, err := c.r.ReadBytes('\n')
	if err != nil {
		return nil
	}
	var f protocol.Frame
	if err := json.Unmarshal(line, &f); err != nil {
		c.t.Fatalf("server sent invalid JSON %q: %v", line, err)
	}
	return &f
}

func (c *raw) expectErr(substr string) {
	c.t.Helper()
	f := c.recv()
	if f == nil || f.Op != protocol.OpErr || !strings.Contains(f.Error, substr) {
		c.t.Fatalf("want err containing %q, got %+v", substr, f)
	}
}

func (c *raw) expectOK() *protocol.Frame {
	c.t.Helper()
	f := c.recv()
	if f == nil || f.Op != protocol.OpOK {
		c.t.Fatalf("want ok, got %+v", f)
	}
	return f
}

func (c *raw) expectClosed() {
	c.t.Helper()
	c.nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.r.ReadByte(); err == nil {
		c.t.Fatal("connection still open")
	}
}

func TestProtocolAbuseKeepsConnectionUsable(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{})
	c := rawDial(t, e.addr)

	// Recoverable errors: the connection must keep working after each.
	cases := []struct{ send, want string }{
		{`not json at all`, "malformed"},
		{`{"op":`, "malformed"},
		{`[1,2,3]`, "malformed"},
		{`{"op":"nope","id":5}`, "unknown op"},
		{`{"op":""}`, "unknown op"},
		{`{"op":"pub","id":1,"topic":"bad topic!","value":"x"}`, "invalid topic"},
		{`{"op":"pub","id":1,"topic":"../etc","value":"x"}`, "invalid topic"},
		{`{"op":"pub","id":1,"topic":"t","value":"!!!","enc":"base64"}`, "bad value encoding"},
		{`{"op":"pub","id":1,"topic":"t","value":"x","partition":-3}`, "invalid partition"},
		{`{"op":"pub","id":1,"topic":"t","value":"x","partition":99}`, "partition out of range"},
		{`{"op":"create","id":1,"topic":"c","partitions":-1}`, "partition count"},
		{`{"op":"create","id":1,"topic":"c","partitions":100000}`, "partition count"},
		{`{"op":"sub","id":1,"topic":"t","from":"banana"}`, "invalid from"},
		{`{"op":"sub","id":1,"topic":"t","from":"committed"}`, "requires a group"},
		{`{"op":"sub","id":1,"topic":"t","group":"g","from":"5"}`, "numeric from cannot"},
		{`{"op":"sub","id":1,"topic":"t","partition":7}`, "partition out of range"},
		{`{"op":"sub","id":1,"topic":"t","from":"999"}`, "beyond log end"},
	}
	// Topic "t" must exist with 1 partition for the sub cases.
	c.send(`{"op":"pub","id":9,"topic":"t","value":"seed"}`)
	c.expectOK()
	for _, tc := range cases {
		c.send(tc.send)
		c.expectErr(tc.want)
	}
	// Ack-related errors come back with id 0.
	c.send(`{"op":"ack","sub":"ghost","offset":1}`)
	c.expectErr("unknown subscription")
	c.send(`{"op":"sub","id":2,"topic":"t","sub":"x","from":"earliest"}`)
	c.expectOK()
	if f := c.recv(); f == nil || f.Op != protocol.OpMsg { // the seed message streams first
		t.Fatalf("want msg, got %+v", f)
	}
	c.send(`{"op":"sub","id":3,"topic":"t","sub":"x"}`)
	c.expectErr("already exists")
	c.send(`{"op":"ack","sub":"x"}`)
	c.expectErr("requires offset")
	c.send(`{"op":"ack","sub":"x","partition":0,"offset":50}`)
	c.expectErr("beyond last delivered")
	// Still alive and well.
	c.send(`{"op":"ping","id":77}`)
	if f := c.recv(); f == nil || f.Op != protocol.OpPong || f.ID != 77 {
		t.Fatalf("ping after abuse: %+v", f)
	}
}

func TestOversizedFrameClosesConnection(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{MaxFrameBytes: 1024})
	c := rawDial(t, e.addr)
	c.send(`{"op":"pub","id":1,"topic":"t","value":"` + strings.Repeat("a", 4096) + `"}`)
	c.expectErr("frame too large")
	c.expectClosed()
	// A frame within the limit is fine on a new connection.
	c2 := rawDial(t, e.addr)
	c2.send(`{"op":"pub","id":1,"topic":"t","value":"` + strings.Repeat("a", 500) + `"}`)
	c2.expectOK()
}

func TestRecordTooLargeRejectedNotFatal(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{Log: commitlog.Options{MaxRecordBytes: 100}}, server.Config{})
	c := dial(t, e.addr)
	ctx := context.Background()
	if _, err := c.Publish(ctx, "t", "", make([]byte, 101)); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("got %v", err)
	}
	if _, err := c.Publish(ctx, "t", "", make([]byte, 100)); err != nil {
		t.Fatalf("limit-sized record rejected: %v", err)
	}
}

func TestBinaryGarbageDoesNotCrash(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{})
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 50; i++ {
		c := rawDial(t, e.addr)
		junk := make([]byte, 1+rng.Intn(2000))
		rng.Read(junk)
		c.nc.Write(junk)
		c.nc.Write([]byte("\n"))
		c.nc.Close()
	}
	good := dial(t, e.addr)
	if err := good.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAuthOrderingEnforced(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{AuthToken: "t0k"})
	c := rawDial(t, e.addr)
	c.send(`{"op":"ping","id":1}`)
	c.expectErr("authentication required")
	c.expectClosed()
	c = rawDial(t, e.addr)
	c.send(`{"op":"auth","id":1,"token":"wrong"}`)
	c.expectErr("invalid token")
	c.expectClosed()
	c = rawDial(t, e.addr)
	c.send(`{"op":"auth","id":1,"token":"t0k"}`)
	c.expectOK()
	c.send(`{"op":"ping","id":2}`)
	if f := c.recv(); f == nil || f.Op != protocol.OpPong {
		t.Fatalf("got %+v", f)
	}
	// A server with no token accepts (and ignores) an auth op.
	e2 := start(t, t.TempDir(), broker.Config{}, server.Config{})
	c = rawDial(t, e2.addr)
	c.send(`{"op":"auth","id":1,"token":"anything"}`)
	c.expectOK()
}

func TestKeepaliveHoldsIdleConnectionOpen(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{IdleTimeout: 250 * time.Millisecond})
	c := dial(t, e.addr, client.WithPingInterval(60*time.Millisecond))
	time.Sleep(900 * time.Millisecond) // > 3x the idle timeout
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("pinging client was dropped: %v", err)
	}
	// The same server drops a silent raw connection.
	r := rawDial(t, e.addr)
	r.expectErr("idle timeout")
}

// goroutines waits for the goroutine count to settle at or below limit.
func settled(limit int) (int, bool) {
	deadline := time.Now().Add(5 * time.Second)
	n := runtime.NumGoroutine()
	for n > limit && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
		n = runtime.NumGoroutine()
	}
	return n, n <= limit
}

func TestNoGoroutineLeakAfterChurn(t *testing.T) {
	time.Sleep(100 * time.Millisecond)
	base := runtime.NumGoroutine()
	{
		e := start(t, t.TempDir(), broker.Config{DefaultPartitions: 3}, server.Config{})
		ctx := context.Background()
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				c, err := client.Dial(ctx, e.addr, client.WithReconnect(time.Second))
				if err != nil {
					t.Error(err)
					return
				}
				for j := 0; j < 5; j++ {
					c.Publish(ctx, "leak", fmt.Sprint(j), []byte("x"))
				}
				s1, _ := c.Subscribe(ctx, "leak", client.SubscribeOptions{Group: "g", From: client.Earliest, Window: 5})
				s2, _ := c.Subscribe(ctx, "leak", client.SubscribeOptions{From: client.Earliest})
				if s1 != nil {
					select {
					case m := <-s1.C:
						s1.Ack(m)
					case <-time.After(time.Second):
					}
					if i%2 == 0 {
						s1.Close()
					}
				}
				if s2 != nil && i%3 == 0 {
					s2.Close()
				}
				c.Close() // closes the rest implicitly
			}(i)
		}
		wg.Wait()
		// Abrupt raw disconnects with live subscriptions.
		for i := 0; i < 10; i++ {
			r := rawDial(t, e.addr)
			r.send(`{"op":"sub","id":1,"topic":"leak","group":"h","from":"earliest","window":2}`)
			r.recv()
			r.nc.Close()
		}
		time.Sleep(100 * time.Millisecond)
		st := e.s.Stats()
		if st["connections"] != 0 || st["subscriptions"] != 0 {
			t.Fatalf("stats after churn: %+v", st)
		}
		e.stop()
	}
	if n, ok := settled(base + 2); !ok {
		buf := make([]byte, 1<<16)
		buf = buf[:runtime.Stack(buf, true)]
		t.Fatalf("goroutines: %d after churn, baseline %d\n%s", n, base, buf)
	}
}

// TestChurnNeverLosesMessages hammers one group with joins/leaves while
// publishers run, and asserts every message is eventually consumed at least
// once and the group ends fully committed.
func TestChurnNeverLosesMessages(t *testing.T) {
	const parts, total = 4, 400
	dir := t.TempDir()
	e := start(t, dir, broker.Config{DefaultPartitions: parts, Log: commitlog.Options{SyncInterval: -1}}, server.Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type pk struct {
		p int
		o int64
	}
	var mu sync.Mutex
	seen := map[pk]int{}
	member := func(life time.Duration) {
		c, err := client.Dial(ctx, e.addr)
		if err != nil {
			return
		}
		defer c.Close()
		s, err := c.Subscribe(ctx, "churn", client.SubscribeOptions{Group: "cg", From: client.Earliest, Window: 20})
		if err != nil {
			return
		}
		var stop <-chan time.Time
		if life > 0 {
			stop = time.After(life)
		}
		for {
			select {
			case m, ok := <-s.C:
				if !ok {
					return
				}
				mu.Lock()
				seen[pk{m.Partition, m.Offset}]++
				mu.Unlock()
				s.Ack(m)
			case <-stop:
				return
			case <-ctx.Done():
				return
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); member(0) }() // permanent member
	// Churners.
	churnCtx, stopChurn := context.WithCancel(ctx)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(i)))
			for churnCtx.Err() == nil {
				member(time.Duration(20+rng.Intn(150)) * time.Millisecond)
			}
		}(i)
	}
	pc := dial(t, e.addr)
	for i := 0; i < total; i++ {
		if _, err := pc.Publish(ctx, "churn", fmt.Sprint(i), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
		if i%40 == 0 {
			time.Sleep(30 * time.Millisecond)
		}
	}
	stopChurn()

	deadline := time.Now().Add(15 * time.Second)
	for {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n == total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d messages consumed", n, total)
		}
		time.Sleep(25 * time.Millisecond)
	}
	// Once only the permanent member remains, the group must converge to zero lag.
	deadline = time.Now().Add(10 * time.Second)
	for {
		lag := int64(0)
		next := map[int]int64{}
		for _, ti := range e.b.Topics() {
			for _, p := range ti.Partitions {
				next[p.Partition] = p.Next
			}
		}
		for p, n := range next {
			c, _ := e.b.Offsets().Get("cg", "churn", p)
			lag += n - c
		}
		if lag == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("group never converged, lag=%d", lag)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	mu.Lock()
	dups := 0
	for _, n := range seen {
		dups += n - 1
	}
	mu.Unlock()
	t.Logf("%d messages, %d redeliveries across rebalances", total, dups)
}

func TestFollowerResetsWhenBehindRetentionAndResumes(t *testing.T) {
	cfg := broker.Config{Log: commitlog.Options{SegmentBytes: 300, RetentionBytes: 600, SyncInterval: -1}}
	leader := start(t, t.TempDir(), cfg, server.Config{})
	ctx := context.Background()
	lc := dial(t, leader.addr)
	for i := 0; i < 200; i++ {
		lc.Publish(ctx, "ret", "", []byte(fmt.Sprintf("message-%03d", i)))
	}
	leader.b.EnforceRetention()
	lt := leader.b.Topics()[0].Partitions[0]
	if lt.Earliest == 0 {
		t.Fatal("leader did not trim")
	}

	folDir := t.TempDir()
	fol := start(t, folDir, broker.Config{Log: commitlog.Options{SyncInterval: -1}}, server.Config{ReadOnly: true})
	rctx, rcancel := context.WithCancel(ctx)
	f := &replica.Follower{B: fol.b, Leader: leader.addr, Logger: quiet, DiscoverInterval: 30 * time.Millisecond}
	done := make(chan struct{})
	go func() { f.Run(rctx); close(done) }()
	waitFor(t, "follower catch-up after reset", func() bool {
		ts := fol.b.Topics()
		return len(ts) == 1 && ts[0].Partitions[0].Next == lt.Next && ts[0].Partitions[0].Earliest == lt.Earliest
	})

	// More leader traffic, then restart the follower process: no gaps, no dups.
	for i := 200; i < 230; i++ {
		lc.Publish(ctx, "ret", "", []byte(fmt.Sprintf("message-%03d", i)))
	}
	waitFor(t, "live tail", func() bool { return fol.b.Topics()[0].Partitions[0].Next == 230 })
	rcancel()
	<-done
	fol.stop()
	for i := 230; i < 250; i++ { // arrives while the follower is down
		lc.Publish(ctx, "ret", "", []byte(fmt.Sprintf("message-%03d", i)))
	}
	fol2 := start(t, folDir, broker.Config{Log: commitlog.Options{SyncInterval: -1}}, server.Config{ReadOnly: true})
	f2 := &replica.Follower{B: fol2.b, Leader: leader.addr, Logger: quiet, DiscoverInterval: 30 * time.Millisecond}
	rctx2, rcancel2 := context.WithCancel(ctx)
	defer rcancel2()
	go f2.Run(rctx2)
	waitFor(t, "resume after restart", func() bool { return fol2.b.Topics()[0].Partitions[0].Next == 250 })

	// Contents match the leader's retained suffix exactly.
	fc := dial(t, fol2.addr)
	sub, _ := fc.Subscribe(ctx, "ret", client.SubscribeOptions{From: client.Earliest})
	want := fol2.b.Topics()[0].Partitions[0]
	for off := want.Earliest; off < 250; off++ {
		m := recv(t, sub)
		if m.Offset != off || string(m.Value) != fmt.Sprintf("message-%03d", off) {
			t.Fatalf("offset %d: got %+v", off, m)
		}
	}
}

func waitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(15 * time.Millisecond)
	}
}

func TestSlowConsumerDoesNotStallOthers(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{Log: commitlog.Options{SyncInterval: -1}},
		server.Config{OutQueue: 8, WriteTimeout: 200 * time.Millisecond})
	ctx := context.Background()
	fast := dial(t, e.addr)
	sub, _ := fast.Subscribe(ctx, "mix", client.SubscribeOptions{Buffer: 1024})

	stuck := rawDial(t, e.addr)
	stuck.nc.(*net.TCPConn).SetReadBuffer(2048)
	stuck.send(`{"op":"sub","id":1,"topic":"mix"}`)
	time.Sleep(50 * time.Millisecond)

	pub := dial(t, e.addr)
	big := bytes.Repeat([]byte("x"), 32<<10)
	var got atomic.Int64
	go func() {
		for range sub.C {
			got.Add(1)
		}
	}()
	start := time.Now()
	for i := 0; i < 300; i++ {
		if _, err := pub.Publish(ctx, "mix", "", big); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d > 8*time.Second {
		t.Fatalf("publishing stalled behind a slow consumer: %v", d)
	}
	waitFor(t, "fast subscriber to receive everything", func() bool { return got.Load() == 300 })
	waitFor(t, "slow consumer disconnect", func() bool { return e.s.Stats()["slow_disconnects"] >= 1 })
}

func TestConcurrentPublishersTotalOrderPerPartition(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{DefaultPartitions: 3, Log: commitlog.Options{SyncInterval: -1}}, server.Config{})
	ctx := context.Background()
	const pubs, each = 8, 100
	var wg sync.WaitGroup
	var rmu sync.Mutex
	got := map[client.Receipt]string{}
	for p := 0; p < pubs; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			c := dial(t, e.addr)
			for i := 0; i < each; i++ {
				v := fmt.Sprintf("%d-%d", p, i)
				r, err := c.Publish(ctx, "conc", v, []byte(v))
				if err != nil {
					t.Error(err)
					return
				}
				rmu.Lock()
				if prev, dup := got[r]; dup {
					t.Errorf("receipt %+v issued twice (%s, %s)", r, prev, v)
				}
				got[r] = v
				rmu.Unlock()
			}
		}(p)
	}
	wg.Wait()
	if len(got) != pubs*each {
		t.Fatalf("%d unique receipts, want %d", len(got), pubs*each)
	}
	// Every publisher's messages to a partition appear in publish order.
	c := dial(t, e.addr)
	sub, _ := c.Subscribe(ctx, "conc", client.SubscribeOptions{From: client.Earliest, Buffer: 2048})
	lastSeq := map[string]int{}
	for i := 0; i < pubs*each; i++ {
		m := recv(t, sub)
		if got[client.Receipt{Partition: m.Partition, Offset: m.Offset}] != string(m.Value) {
			t.Fatalf("receipt/content mismatch at %d/%d", m.Partition, m.Offset)
		}
		var p, seq int
		fmt.Sscanf(string(m.Value), "%d-%d", &p, &seq)
		key := fmt.Sprintf("%d@%d", p, m.Partition)
		if prev, ok := lastSeq[key]; ok && seq < prev {
			t.Fatalf("publisher %d reordered within partition %d", p, m.Partition)
		}
		lastSeq[key] = seq
	}
}
