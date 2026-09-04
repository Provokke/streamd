package server_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Provokke/streamd/broker"
	"github.com/Provokke/streamd/client"
	"github.com/Provokke/streamd/commitlog"
	"github.com/Provokke/streamd/server"
)

func TestPublishBatchEndToEnd(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{DefaultPartitions: 3, Log: commitlog.Options{SyncInterval: -1}}, server.Config{})
	ctx := context.Background()
	c := dial(t, e.addr)
	recs := make([]client.Record, 90)
	for i := range recs {
		recs[i] = client.Record{Key: fmt.Sprintf("k%d", i%9), Value: []byte(fmt.Sprint(i))}
	}
	recs[10].Value = []byte{0xff, 0x00, 0xfe} // binary payload in the middle of a batch
	recs[11].Key = string([]byte{0xc3, 0x28})
	rs, err := c.PublishBatch(ctx, "b", recs)
	if err != nil || len(rs) != len(recs) {
		t.Fatalf("%d receipts, err %v", len(rs), err)
	}
	sub, _ := c.Subscribe(ctx, "b", client.SubscribeOptions{From: client.Earliest, Buffer: 256})
	byReceipt := map[client.Receipt]client.Message{}
	for range recs {
		m := recv(t, sub)
		byReceipt[client.Receipt{Partition: m.Partition, Offset: m.Offset}] = m
	}
	for i, r := range rs {
		m, ok := byReceipt[r]
		if !ok || m.Key != recs[i].Key || string(m.Value) != string(recs[i].Value) {
			t.Fatalf("record %d: receipt %+v -> %+v", i, r, m)
		}
	}
	// Same key always maps to the same partition, matching single publish.
	single, _ := c.Publish(ctx, "b", "k3", []byte("single"))
	for i, r := range rs {
		if recs[i].Key == "k3" && r.Partition != single.Partition {
			t.Fatalf("batch and single publish disagree on partition for k3")
		}
	}
	// A whole batch to one partition is contiguous.
	ps, err := c.PublishBatchTo(ctx, "b", 1, recs[:5])
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(ps); i++ {
		if ps[i].Partition != 1 || ps[i].Offset != ps[i-1].Offset+1 {
			t.Fatalf("not contiguous: %+v", ps)
		}
	}
	st, _ := c.Stats(ctx)
	if st["messages_in"] != int64(len(recs)+1+5) {
		t.Fatalf("messages_in=%d", st["messages_in"])
	}
}

func TestPublishBatchErrors(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{DefaultPartitions: 2, Log: commitlog.Options{MaxRecordBytes: 50}}, server.Config{})
	ctx := context.Background()
	c := dial(t, e.addr)
	if _, err := c.PublishBatch(ctx, "x", nil); err == nil || !strings.Contains(err.Error(), "empty batch") {
		t.Fatalf("empty: %v", err)
	}
	big := make([]client.Record, 10001)
	if _, err := c.PublishBatch(ctx, "x", big); err == nil || !strings.Contains(err.Error(), "batch too large") {
		t.Fatalf("huge: %v", err)
	}
	bad := []client.Record{{Value: []byte("fine")}, {Value: make([]byte, 51)}}
	if _, err := c.PublishBatch(ctx, "x", bad); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("oversize: %v", err)
	}
	if _, err := c.PublishBatchTo(ctx, "x", 5, bad[:1]); err == nil {
		t.Fatal("bad partition accepted")
	}
	// Nothing was written by any rejected batch.
	for _, ti := range e.b.Topics() {
		for _, p := range ti.Partitions {
			if p.Next != 0 {
				t.Fatalf("rejected batches wrote data: %+v", ti)
			}
		}
	}
	// Raw frames: bad encodings in a batch.
	r := rawDial(t, e.addr)
	r.send(`{"op":"pubbatch","id":1,"topic":"x","msgs":[{"value":"!!","enc":"base64"}]}`)
	r.expectErr("bad encoding")
	r.send(`{"op":"pubbatch","id":2,"topic":"x","msgs":[{"key":"!!","kenc":"base64","value":"v"}]}`)
	r.expectErr("bad encoding")
}

func TestReadOnlyFollowerRejectsAdminWrites(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{}, server.Config{ReadOnly: true})
	r := rawDial(t, e.addr)
	for _, f := range []string{
		`{"op":"pubbatch","id":1,"topic":"t","msgs":[{"value":"x"}]}`,
		`{"op":"delete","id":2,"topic":"t"}`,
		`{"op":"reset","id":3,"topic":"t","group":"g"}`,
		`{"op":"create","id":4,"topic":"t"}`,
		`{"op":"pub","id":5,"topic":"t","value":"x"}`,
	} {
		r.send(f)
		r.expectErr("read-only")
	}
}

func TestDeleteTopicEndsSubscriptions(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{DefaultPartitions: 2}, server.Config{})
	ctx := context.Background()
	c := dial(t, e.addr)
	for i := 0; i < 6; i++ {
		c.PublishTo(ctx, "del", i%2, "", []byte("x"))
	}
	plain, _ := c.Subscribe(ctx, "del", client.SubscribeOptions{From: client.Earliest})
	grp, _ := c.Subscribe(ctx, "del", client.SubscribeOptions{Group: "dg", From: client.Earliest})
	for i := 0; i < 6; i++ {
		recv(t, plain)
		grp.Ack(recv(t, grp))
	}
	time.Sleep(50 * time.Millisecond)

	admin := dial(t, e.addr)
	if err := admin.DeleteTopic(ctx, "del"); err != nil {
		t.Fatal(err)
	}
	for name, s := range map[string]*client.Subscription{"plain": plain, "group": grp} {
		select {
		case _, ok := <-s.C:
			if ok {
				t.Fatalf("%s: unexpected message", name)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("%s subscription not ended by topic deletion", name)
		}
		if err := s.Err(); err == nil || !strings.Contains(err.Error(), "topic deleted") {
			t.Fatalf("%s err = %v", name, err)
		}
	}
	waitFor(t, "server to drop the subscriptions", func() bool { return e.s.Stats()["subscriptions"] == 0 })
	if err := admin.DeleteTopic(ctx, "del"); err == nil {
		t.Fatal("deleting a missing topic should fail")
	}
	// Recreating starts from offset 0, and the old group's position is gone.
	r, _ := c.PublishTo(ctx, "del", 0, "", []byte("new"))
	if r.Offset != 0 {
		t.Fatalf("recreated topic offset %d", r.Offset)
	}
	g2, _ := c.Subscribe(ctx, "del", client.SubscribeOptions{Group: "dg", From: client.Earliest})
	if m := recv(t, g2); string(m.Value) != "new" || m.Offset != 0 {
		t.Fatalf("group resumed from stale offset: %+v", m)
	}
}

func TestResetOffsetsThenResume(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{DefaultPartitions: 2}, server.Config{})
	ctx := context.Background()
	c := dial(t, e.addr)
	for i := 0; i < 20; i++ {
		c.PublishTo(ctx, "rs", i%2, "", []byte(fmt.Sprint(i)))
	}
	sub, _ := c.Subscribe(ctx, "rs", client.SubscribeOptions{Group: "rg", From: client.Earliest})
	var last [2]client.Message
	for i := 0; i < 20; i++ {
		m := recv(t, sub)
		last[m.Partition] = m
	}
	sub.Ack(last[0])
	sub.Ack(last[1])
	waitFor(t, "group fully committed", func() bool {
		gs, _ := c.Groups(ctx, "rs", "rg")
		return len(gs) == 2 && gs[0].Lag == 0 && gs[1].Lag == 0 && gs[0].Members == 1
	})

	// With a live member the reset is refused.
	if _, err := c.ResetOffsets(ctx, "rs", "rg", client.Earliest, nil); err == nil ||
		!strings.Contains(err.Error(), "active members") {
		t.Fatalf("got %v", err)
	}
	sub.Close()
	c2 := dial(t, e.addr)
	waitFor(t, "member to leave", func() bool {
		gs, _ := c2.Groups(ctx, "rs", "rg")
		return len(gs) == 2 && gs[0].Members == 0
	})

	// Rewind one partition to offset 7, the other to earliest: replay exactly the tail / all.
	if offs, err := c2.ResetOffsets(ctx, "rs", "rg", client.At(7), client.Partition(0)); err != nil || offs[0] != 7 {
		t.Fatalf("reset p0: %v %v", offs, err)
	}
	if offs, err := c2.ResetOffsets(ctx, "rs", "rg", client.Earliest, client.Partition(1)); err != nil || offs[1] != 0 {
		t.Fatalf("reset p1: %v %v", offs, err)
	}
	gs, _ := c2.Groups(ctx, "rs", "rg")
	if gs[0].Committed != 7 || gs[0].Lag != 3 || gs[1].Committed != 0 || gs[1].Lag != 10 {
		t.Fatalf("groups after reset: %+v", gs)
	}
	sub2, err := c2.Subscribe(ctx, "rs", client.SubscribeOptions{Group: "rg"})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[int]int{}
	first := map[int]int64{}
	for i := 0; i < 13; i++ { // 3 from p0 (offsets 7..9) + 10 from p1
		m := recv(t, sub2)
		if _, ok := first[m.Partition]; !ok {
			first[m.Partition] = m.Offset
		}
		counts[m.Partition]++
	}
	if counts[0] != 3 || counts[1] != 10 || first[0] != 7 || first[1] != 0 {
		t.Fatalf("replay after reset: counts=%v first=%v", counts, first)
	}
	// "latest" skips everything; errors for bad targets.
	sub2.Close()
	waitFor(t, "member to leave", func() bool { gs, _ := c2.Groups(ctx, "rs", "rg"); return gs[0].Members == 0 })
	if _, err := c2.ResetOffsets(ctx, "rs", "rg", client.Latest, nil); err != nil {
		t.Fatal(err)
	}
	if gs, _ := c2.Groups(ctx, "rs", "rg"); gs[0].Lag != 0 || gs[1].Lag != 0 {
		t.Fatalf("latest left lag: %+v", gs)
	}
	if _, err := c2.ResetOffsets(ctx, "rs", "rg", "banana", nil); err == nil {
		t.Fatal("bad target accepted")
	}
	if _, err := c2.ResetOffsets(ctx, "ghost", "rg", client.Earliest, nil); err == nil {
		t.Fatal("missing topic accepted")
	}
}

func TestGroupsFilterAndMembers(t *testing.T) {
	e := start(t, t.TempDir(), broker.Config{DefaultPartitions: 2}, server.Config{})
	ctx := context.Background()
	c := dial(t, e.addr)
	c.Publish(ctx, "ga", "", []byte("x"))
	c.Publish(ctx, "gb", "", []byte("x"))
	s1, _ := c.Subscribe(ctx, "ga", client.SubscribeOptions{Group: "one", From: client.Earliest})
	s2, _ := dial(t, e.addr).Subscribe(ctx, "ga", client.SubscribeOptions{Group: "one", From: client.Earliest})
	s3, _ := c.Subscribe(ctx, "gb", client.SubscribeOptions{Group: "two", From: client.Earliest})
	waitAssign(t, s1, 1)
	waitAssign(t, s2, 1)
	waitAssign(t, s3, 2)
	all, _ := c.Groups(ctx, "", "")
	if len(all) != 4 {
		t.Fatalf("all: %+v", all)
	}
	only, _ := c.Groups(ctx, "ga", "")
	if len(only) != 2 || only[0].Members != 2 {
		t.Fatalf("topic filter: %+v", only)
	}
	none, _ := c.Groups(ctx, "ga", "two")
	if len(none) != 0 {
		t.Fatalf("group filter: %+v", none)
	}
}
