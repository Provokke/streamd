package server_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Provokke/streamd/broker"
	"github.com/Provokke/streamd/client"
	"github.com/Provokke/streamd/commitlog"
	"github.com/Provokke/streamd/replica"
	"github.com/Provokke/streamd/server"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type env struct {
	b    *broker.Broker
	s    *server.Server
	addr string
}

func start(t testing.TB, dir string, bc broker.Config, sc server.Config) *env {
	t.Helper()
	return startAt(t, "127.0.0.1:0", dir, bc, sc)
}

func startAt(t testing.TB, addr, dir string, bc broker.Config, sc server.Config) *env {
	t.Helper()
	bc.Dir, bc.Logger = dir, quiet
	b, err := broker.Open(bc)
	if err != nil {
		t.Fatal(err)
	}
	sc.Logger = quiet
	s := server.New(b, sc)
	var ln net.Listener
	for i := 0; ; i++ { // a just-closed port may take a moment to free up
		if ln, err = net.Listen("tcp", addr); err == nil {
			break
		}
		if i > 50 {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	go s.Serve(ln)
	e := &env{b: b, s: s, addr: ln.Addr().String()}
	t.Cleanup(e.stop)
	return e
}

func (e *env) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	e.s.Shutdown(ctx)
	e.b.Close()
}

func dial(t testing.TB, addr string, opts ...client.Option) *client.Client {
	t.Helper()
	c, err := client.Dial(context.Background(), addr, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func recv(t *testing.T, s *client.Subscription) client.Message {
	t.Helper()
	select {
	case m, ok := <-s.C:
		if !ok {
			t.Fatalf("subscription closed: %v", s.Err())
		}
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
	}
	return client.Message{}
}

func TestPubSubLiveAndReplay(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{})
	ctx := context.Background()
	c := dial(t, e.addr)

	for i := 0; i < 5; i++ {
		r, err := c.Publish(ctx, "orders", "k", []byte(fmt.Sprintf("m%d", i)))
		if err != nil || r.Offset != int64(i) || r.Partition != 0 {
			t.Fatalf("publish: %+v %v", r, err)
		}
	}
	// Replay from earliest, then receive live.
	sub, err := c.Subscribe(ctx, "orders", client.SubscribeOptions{From: client.Earliest})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if m := recv(t, sub); m.Offset != int64(i) || string(m.Value) != fmt.Sprintf("m%d", i) || m.Key != "k" {
			t.Fatalf("bad replay %+v", m)
		}
	}
	c2 := dial(t, e.addr)
	c2.Publish(ctx, "orders", "", []byte{0xff, 0xfe, 0x00}) // binary payload
	if m := recv(t, sub); m.Offset != 5 || len(m.Value) != 3 || m.Value[0] != 0xff {
		t.Fatalf("bad live msg %+v", m)
	}
	// Default (no group) starts at latest.
	late, _ := c.Subscribe(ctx, "orders", client.SubscribeOptions{})
	c2.Publish(ctx, "orders", "", []byte("new"))
	if m := recv(t, late); string(m.Value) != "new" {
		t.Fatalf("latest sub got %+v", m)
	}
}

func TestGroupCommitResumeAndIdleMember(t *testing.T) {
	dir := t.TempDir()
	e := start(t, dir, broker.Config{}, server.Config{})
	ctx := context.Background()
	c := dial(t, e.addr)
	for i := 0; i < 10; i++ {
		c.Publish(ctx, "t", "", []byte(fmt.Sprint(i)))
	}
	sub, err := c.Subscribe(ctx, "t", client.SubscribeOptions{Group: "g"})
	if err != nil {
		t.Fatal(err)
	}
	// A single-partition topic has nothing to share: a second member joins
	// but idles with an empty assignment, and the first keeps the partition.
	idle, err := dial(t, e.addr).Subscribe(ctx, "t", client.SubscribeOptions{Group: "g"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if len(idle.Assignment()) != 0 || len(sub.Assignment()) != 1 {
		t.Fatalf("assignments: idle=%v sub=%v", idle.Assignment(), sub.Assignment())
	}
	idle.Close()
	var last client.Message
	for i := 0; i < 4; i++ {
		last = recv(t, sub)
	}
	sub.Ack(last) // commits offset 4 (next to read)
	sub.Close()
	time.Sleep(50 * time.Millisecond)

	// Restart the whole broker: the committed offset must survive.
	e.stop()
	e2 := start(t, dir, broker.Config{}, server.Config{})
	c = dial(t, e2.addr)
	sub, err = c.Subscribe(ctx, "t", client.SubscribeOptions{Group: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if m := recv(t, sub); m.Offset != 4 || string(m.Value) != "4" {
		t.Fatalf("resumed at %+v, want offset 4", m)
	}
}

func TestWindowFlowControl(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{})
	ctx := context.Background()
	c := dial(t, e.addr)
	for i := 0; i < 20; i++ {
		c.Publish(ctx, "w", "", []byte("x"))
	}
	sub, _ := c.Subscribe(ctx, "w", client.SubscribeOptions{From: client.Earliest, Window: 3})
	var got []client.Message
	for i := 0; i < 3; i++ {
		got = append(got, recv(t, sub))
	}
	select {
	case m := <-sub.C:
		t.Fatalf("server exceeded window: %+v", m)
	case <-time.After(200 * time.Millisecond):
	}
	sub.Ack(got[2]) // frees 3 credits
	for i := 0; i < 3; i++ {
		if m := recv(t, sub); m.Offset != int64(3+i) {
			t.Fatalf("got offset %d", m.Offset)
		}
	}
}

func TestAuth(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{AuthToken: "s3cret"})
	if _, err := client.Dial(context.Background(), e.addr, client.WithToken("nope")); err == nil {
		t.Fatal("bad token accepted")
	}
	c := dial(t, e.addr, client.WithToken("s3cret"))
	if _, err := c.Publish(context.Background(), "a", "", []byte("v")); err != nil {
		t.Fatal(err)
	}
	// Raw socket without auth is cut off.
	nc, _ := net.Dial("tcp", e.addr)
	defer nc.Close()
	fmt.Fprintln(nc, `{"op":"pub","topic":"a","value":"v"}`)
	line, _ := bufio.NewReader(nc).ReadString('\n')
	if !strings.Contains(line, "authentication required") {
		t.Fatalf("got %q", line)
	}
}

func TestIdleTimeoutAndMaxConns(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{IdleTimeout: 150 * time.Millisecond, MaxConns: 2})
	a, _ := net.Dial("tcp", e.addr)
	b, _ := net.Dial("tcp", e.addr)
	defer a.Close()
	defer b.Close()
	time.Sleep(50 * time.Millisecond)
	c, _ := net.Dial("tcp", e.addr)
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, _ := bufio.NewReader(c).ReadString('\n')
	if !strings.Contains(line, "too many connections") {
		t.Fatalf("third conn: %q", line)
	}
	a.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, _ = bufio.NewReader(a).ReadString('\n')
	if !strings.Contains(line, "idle timeout") {
		t.Fatalf("idle conn: %q", line)
	}
	st := e.s.Stats()
	if st["idle_disconnects"] < 1 || st["connections_rejected"] != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestSlowConsumerDisconnected(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{Log: commitlog.Options{SyncInterval: -1}}, server.Config{OutQueue: 8, WriteTimeout: 300 * time.Millisecond})
	ctx := context.Background()
	pub := dial(t, e.addr)
	big := []byte(strings.Repeat("x", 64<<10))

	// A raw client that subscribes and then never reads.
	nc, err := net.Dial("tcp", e.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	nc.(*net.TCPConn).SetReadBuffer(4096)
	fmt.Fprintln(nc, `{"op":"sub","id":1,"topic":"slow","from":"latest"}`)
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 400; i++ {
		if _, err := pub.Publish(ctx, "slow", "", big); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for e.s.Stats()["slow_disconnects"] == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if e.s.Stats()["slow_disconnects"] == 0 {
		t.Fatalf("slow consumer was not disconnected: %+v", e.s.Stats())
	}
	if err := pub.Ping(ctx); err != nil { // others unaffected
		t.Fatal(err)
	}
}

func TestShutdownClosesClients(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{})
	c := dial(t, e.addr)
	sub, _ := c.Subscribe(context.Background(), "x", client.SubscribeOptions{})
	e.stop()
	select {
	case _, ok := <-sub.C:
		if ok {
			t.Fatal("unexpected message")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscription not closed on shutdown")
	}
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("client not notified")
	}
}

func TestRetentionSkipsSlowSubscriber(t *testing.T) {
	e := start(t, t.TempDir(),
		broker.Config{Log: commitlog.Options{SegmentBytes: 200, RetentionBytes: 400, SyncInterval: -1}},
		server.Config{})
	ctx := context.Background()
	c := dial(t, e.addr)
	for i := 0; i < 100; i++ {
		c.Publish(ctx, "r", "", []byte("0123456789"))
	}
	e.b.EnforceRetention()
	ts, _ := c.Topics(ctx)
	if len(ts) != 1 || ts[0].Partitions[0].Earliest == 0 {
		t.Fatalf("retention did not trim: %+v", ts)
	}
	earliest := ts[0].Partitions[0].Earliest
	// Asking for offset 0 clamps to the earliest retained offset.
	sub, err := c.Subscribe(ctx, "r", client.SubscribeOptions{From: client.At(0)})
	if err != nil {
		t.Fatal(err)
	}
	if m := recv(t, sub); m.Offset != earliest {
		t.Fatalf("got %d want %d", m.Offset, earliest)
	}
}

func TestFollowerReplication(t *testing.T) {
	leader := start(t, t.TempDir(), broker.Config{}, server.Config{})
	fol := start(t, t.TempDir(), broker.Config{}, server.Config{ReadOnly: true})
	ctx := context.Background()
	lc := dial(t, leader.addr)
	for i := 0; i < 50; i++ {
		lc.Publish(ctx, "rep", "k", []byte(fmt.Sprint(i)))
	}
	f := &replica.Follower{B: fol.b, Leader: leader.addr, Logger: quiet, DiscoverInterval: 50 * time.Millisecond}
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go f.Run(rctx)

	fc := dial(t, fol.addr)
	if _, err := fc.Publish(ctx, "rep", "", []byte("no")); err == nil {
		t.Fatal("follower accepted a publish")
	}
	for i := 50; i < 60; i++ { // also a live tail
		lc.Publish(ctx, "rep", "k", []byte(fmt.Sprint(i)))
	}
	sub, err := waitSub(fc, "rep")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 60; i++ {
		if m := recv(t, sub); m.Offset != int64(i) || string(m.Value) != fmt.Sprint(i) {
			t.Fatalf("follower msg %d: %+v", i, m)
		}
	}
}

// waitSub retries until the follower has created the topic and caught up.
func waitSub(c *client.Client, topic string) (*client.Subscription, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		ts, _ := c.Topics(context.Background())
		for _, ti := range ts {
			if ti.Name == topic && len(ti.Partitions) == 1 && ti.Partitions[0].Next >= 60 {
				return c.Subscribe(context.Background(), topic, client.SubscribeOptions{From: client.Earliest})
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("follower never caught up")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestManyConcurrentClients(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{Log: commitlog.Options{SyncInterval: -1}}, server.Config{})
	ctx := context.Background()
	const subs, msgs = 25, 200
	var ss []*client.Subscription
	for i := 0; i < subs; i++ {
		c := dial(t, e.addr)
		s, err := c.Subscribe(ctx, "fan", client.SubscribeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		ss = append(ss, s)
	}
	pc := dial(t, e.addr)
	for i := 0; i < msgs; i++ {
		if _, err := pc.Publish(ctx, "fan", "", []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	for _, s := range ss {
		for i := 0; i < msgs; i++ {
			if m := recv(t, s); m.Offset != int64(i) {
				t.Fatalf("order violated: %d != %d", m.Offset, i)
			}
		}
	}
}

// ---- partitions & rebalancing ----

func collect(t *testing.T, s *client.Subscription, n int) []client.Message {
	t.Helper()
	var out []client.Message
	for len(out) < n {
		out = append(out, recv(t, s))
	}
	return out
}

func TestPartitionedPublishAndOrdering(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{})
	ctx := context.Background()
	c := dial(t, e.addr)
	if err := c.CreateTopic(ctx, "p", 4); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateTopic(ctx, "p", 4); err != nil { // idempotent
		t.Fatal(err)
	}
	if err := c.CreateTopic(ctx, "p", 5); err == nil {
		t.Fatal("changing partition count must fail")
	}
	// Same key -> same partition, in order.
	keyPart := map[string]int{}
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("key%d", i%8)
		r, err := c.Publish(ctx, "p", k, []byte(fmt.Sprint(i)))
		if err != nil {
			t.Fatal(err)
		}
		if p, ok := keyPart[k]; ok && p != r.Partition {
			t.Fatalf("key %s moved from partition %d to %d", k, p, r.Partition)
		}
		keyPart[k] = r.Partition
	}
	// Unkeyed messages spread across all partitions.
	seen := map[int]bool{}
	for i := 0; i < 8; i++ {
		r, _ := c.Publish(ctx, "p", "", []byte("u"))
		seen[r.Partition] = true
	}
	if len(seen) != 4 {
		t.Fatalf("round-robin hit %d partitions", len(seen))
	}
	if _, err := c.PublishTo(ctx, "p", 9, "", []byte("x")); err == nil {
		t.Fatal("expected partition range error")
	}
	// Ungrouped subscribe on all partitions: per-partition offsets are dense and ordered.
	sub, err := c.Subscribe(ctx, "p", client.SubscribeOptions{From: client.Earliest})
	if err != nil {
		t.Fatal(err)
	}
	if len(sub.Assignment()) != 4 {
		t.Fatalf("assignment %v", sub.Assignment())
	}
	next := map[int]int64{}
	for _, m := range collect(t, sub, 48) {
		if m.Offset != next[m.Partition] {
			t.Fatalf("partition %d: got offset %d want %d", m.Partition, m.Offset, next[m.Partition])
		}
		next[m.Partition]++
	}
	// Single partition + numeric offset.
	one, err := c.Subscribe(ctx, "p", client.SubscribeOptions{Partition: client.Partition(1), From: client.At(1)})
	if err != nil {
		t.Fatal(err)
	}
	if m := recv(t, one); m.Partition != 1 || m.Offset != 1 {
		t.Fatalf("got %+v", m)
	}
	if _, err := c.Subscribe(ctx, "p", client.SubscribeOptions{From: client.At(1)}); err == nil {
		t.Fatal("numeric from without partition must fail on multi-partition topic")
	}
}

func waitAssign(t *testing.T, s *client.Subscription, n int) []int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if a := s.Assignment(); len(a) == n {
			return a
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("assignment never reached %d partitions: %v", n, s.Assignment())
	return nil
}

func TestGroupRebalance(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{})
	ctx := context.Background()
	pc := dial(t, e.addr)
	pc.CreateTopic(ctx, "g", 4)
	for i := 0; i < 40; i++ {
		pc.PublishTo(ctx, "g", i%4, "", []byte(fmt.Sprint(i)))
	}

	c1 := dial(t, e.addr)
	s1, err := c1.Subscribe(ctx, "g", client.SubscribeOptions{Group: "grp", Window: 100})
	if err != nil {
		t.Fatal(err)
	}
	waitAssign(t, s1, 4)

	// A second member joins: partitions split 2/2, disjoint.
	c2 := dial(t, e.addr)
	s2, err := c2.Subscribe(ctx, "g", client.SubscribeOptions{Group: "grp", Window: 100})
	if err != nil {
		t.Fatal(err)
	}
	a1, a2 := waitAssign(t, s1, 2), waitAssign(t, s2, 2)
	owned := map[int]bool{}
	for _, p := range append(append([]int{}, a1...), a2...) {
		if owned[p] {
			t.Fatalf("partition %d assigned twice: %v %v", p, a1, a2)
		}
		owned[p] = true
	}
	if len(owned) != 4 {
		t.Fatalf("not all partitions covered: %v %v", a1, a2)
	}

	// Ack everything s2 receives for its partitions; s1 leaves; s2 takes over all.
	// New messages published now must all arrive at s2 after the rebalance.
	go func() {
		for m := range s2.C {
			s2.Ack(m)
		}
	}()
	time.Sleep(200 * time.Millisecond)
	s1.Close()
	c1.Close()
	waitAssign(t, s2, 4)
	// Every partition is still consumable by the survivor.
	for p := 0; p < 4; p++ {
		pc.PublishTo(ctx, "g", p, "", []byte("after"))
	}
	time.Sleep(300 * time.Millisecond)
	st, _ := pc.Stats(ctx)
	if st["subscriptions"] != 1 {
		t.Fatalf("subscriptions=%d", st["subscriptions"])
	}
}

func TestGroupCommitsPerPartition(t *testing.T) {
	dir := t.TempDir()
	e := start(t, dir, broker.Config{}, server.Config{})
	ctx := context.Background()
	c := dial(t, e.addr)
	c.CreateTopic(ctx, "cp", 2)
	for i := 0; i < 10; i++ {
		c.PublishTo(ctx, "cp", i%2, "", []byte(fmt.Sprint(i)))
	}
	sub, err := c.Subscribe(ctx, "cp", client.SubscribeOptions{Group: "g", From: client.Earliest})
	if err != nil {
		t.Fatal(err)
	}
	got := map[int][]client.Message{}
	for _, m := range collect(t, sub, 10) {
		got[m.Partition] = append(got[m.Partition], m)
	}
	sub.Ack(got[0][2]) // partition 0: acked through offset 2 -> resume at 3
	sub.Ack(got[1][0]) // partition 1: acked through offset 0 -> resume at 1
	time.Sleep(50 * time.Millisecond)
	sub.Close()
	e.stop()

	e2 := start(t, dir, broker.Config{}, server.Config{})
	c = dial(t, e2.addr)
	sub, err = c.Subscribe(ctx, "cp", client.SubscribeOptions{Group: "g"})
	if err != nil {
		t.Fatal(err)
	}
	first := map[int]int64{}
	for _, m := range collect(t, sub, 6) { // 2 left on p0 (3,4), 4 on p1 (1..4)
		if _, ok := first[m.Partition]; !ok {
			first[m.Partition] = m.Offset
		}
	}
	if first[0] != 3 || first[1] != 1 {
		t.Fatalf("resume offsets %v, want p0=3 p1=1", first)
	}
}

func TestFollowerReplicatesPartitions(t *testing.T) {
	leader := start(t, t.TempDir(), broker.Config{}, server.Config{})
	fol := start(t, t.TempDir(), broker.Config{}, server.Config{ReadOnly: true})
	ctx := context.Background()
	lc := dial(t, leader.addr)
	lc.CreateTopic(ctx, "mp", 3)
	for i := 0; i < 30; i++ {
		lc.PublishTo(ctx, "mp", i%3, "k", []byte(fmt.Sprint(i)))
	}
	f := &replica.Follower{B: fol.b, Leader: leader.addr, Logger: quiet, DiscoverInterval: 50 * time.Millisecond}
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go f.Run(rctx)

	deadline := time.Now().Add(5 * time.Second)
	for {
		ts := fol.b.Topics()
		if len(ts) == 1 && len(ts[0].Partitions) == 3 &&
			ts[0].Partitions[0].Next == 10 && ts[0].Partitions[1].Next == 10 && ts[0].Partitions[2].Next == 10 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower state: %+v", ts)
		}
		time.Sleep(20 * time.Millisecond)
	}
	fc := dial(t, fol.addr)
	sub, _ := fc.Subscribe(ctx, "mp", client.SubscribeOptions{Partition: client.Partition(2), From: client.Earliest})
	if m := recv(t, sub); m.Partition != 2 || string(m.Value) != "2" {
		t.Fatalf("got %+v", m)
	}
}

func TestClientReconnectResumes(t *testing.T) {
	dir := t.TempDir()
	e := start(t, dir, broker.Config{}, server.Config{AuthToken: "tok"})
	addr := e.addr
	ctx := context.Background()

	var disc, reco atomic.Int32
	c, err := client.Dial(ctx, addr, client.WithToken("tok"), client.WithReconnect(200*time.Millisecond),
		client.WithConnectionHooks(func(error) { disc.Add(1) }, func() { reco.Add(1) }))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.CreateTopic(ctx, "rc", 2)
	for i := 0; i < 6; i++ {
		c.PublishTo(ctx, "rc", i%2, "", []byte(fmt.Sprint(i)))
	}
	plain, _ := c.Subscribe(ctx, "rc", client.SubscribeOptions{From: client.Earliest})
	grp, _ := c.Subscribe(ctx, "rc", client.SubscribeOptions{Group: "g", From: client.Earliest})
	for _, m := range collect(t, plain, 6) {
		_ = m
	}
	gm := collect(t, grp, 6)
	for _, m := range gm[:4] { // ack only some: unacked ones must be redelivered
		grp.Ack(m)
	}
	time.Sleep(300 * time.Millisecond) // let the offset flusher run

	// Kill the server (clean shutdown flushes offsets) and bring it back on the same port.
	e.stop()
	// While the server is down calls wait for the reconnect, so a bounded ctx times out.
	dctx, dcancel := context.WithTimeout(ctx, 400*time.Millisecond)
	if _, err := c.Publish(dctx, "rc", "", []byte("x")); err == nil {
		t.Fatal("publish should not succeed while the server is down")
	}
	dcancel()
	e2 := startAt(t, addr, dir, broker.Config{}, server.Config{AuthToken: "tok"})
	_ = e2

	deadline := time.Now().Add(10 * time.Second)
	for c.Reconnects() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("never reconnected")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if disc.Load() < 1 || reco.Load() < 1 {
		t.Fatalf("hooks: disconnect=%d reconnect=%d", disc.Load(), reco.Load())
	}

	// Ungrouped sub resumes after the last delivered message: new publishes only, no replay.
	c.PublishTo(ctx, "rc", 0, "", []byte("new0"))
	c.PublishTo(ctx, "rc", 1, "", []byte("new1"))
	got := map[string]bool{}
	for _, m := range collect(t, plain, 2) {
		got[string(m.Value)] = true
	}
	if !got["new0"] || !got["new1"] {
		t.Fatalf("ungrouped resume got %v", got)
	}
	// Grouped sub resumes at the committed offsets: redelivers the unacked messages, then the new ones.
	seen := map[string]bool{}
	for _, m := range collect(t, grp, 2+2) { // 2 unacked + 2 new
		seen[string(m.Value)] = true
	}
	if !seen["new0"] || !seen["new1"] {
		t.Fatalf("group resume missing new messages: %v", seen)
	}
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestClientWithoutReconnectFinishes(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{})
	c := dial(t, e.addr)
	e.stop()
	select {
	case <-c.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("client did not finish")
	}
	if c.Err() == nil {
		t.Fatal("expected terminal error")
	}
}

func TestAdminEndpoints(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{})
	ctx := context.Background()
	c := dial(t, e.addr)
	c.CreateTopic(ctx, "m", 2)
	for i := 0; i < 5; i++ {
		c.PublishTo(ctx, "m", 0, "", []byte("x"))
	}
	sub, _ := c.Subscribe(ctx, "m", client.SubscribeOptions{Group: `we"ird`, From: client.Earliest})
	var last client.Message
	for i := 0; i < 3; i++ {
		last = recv(t, sub)
	}
	sub.Ack(last) // lag should become 5-3 = 2
	time.Sleep(50 * time.Millisecond)

	h := e.s.AdminHandler()
	get := func(path string) (int, string) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		return rec.Code, rec.Body.String()
	}
	if code, body := get("/healthz"); code != 200 || !strings.Contains(body, "ok") {
		t.Fatalf("healthz %d %q", code, body)
	}
	_, body := get("/metrics")
	for _, want := range []string{
		"streamd_connections 1",
		"streamd_messages_in_total 5",
		`streamd_partition_next_offset{topic="m",partition="0"} 5`,
		`streamd_partition_next_offset{topic="m",partition="1"} 0`,
		`streamd_group_lag{group="we\"ird",topic="m",partition="0"} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q\n%s", want, body)
		}
	}
	e.s.Shutdown(ctx)
	if code, _ := get("/healthz"); code != 503 {
		t.Fatalf("healthz after shutdown = %d", code)
	}
}
