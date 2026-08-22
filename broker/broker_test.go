package broker

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Provokke/streamd/commitlog"
)

func open(t *testing.T, dir string, cfg Config) *Broker {
	t.Helper()
	cfg.Dir = dir
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg.Log.SyncInterval = -1
	b, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestTopicNameValidation(t *testing.T) {
	b := open(t, t.TempDir(), Config{})
	defer b.Close()
	for _, bad := range []string{"", ".", "..", "a/b", "a b", "../x", "ü", string(make([]byte, 129))} {
		if _, err := b.Topic(bad, true); err != ErrInvalidTopic {
			t.Errorf("%q: got %v, want ErrInvalidTopic", bad, err)
		}
	}
	if _, err := b.Topic("ok.topic_1-x", true); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Topic("missing", false); err != ErrNoTopic {
		t.Fatalf("got %v", err)
	}
}

func TestPartitioningStableAndPersistent(t *testing.T) {
	dir := t.TempDir()
	b := open(t, dir, Config{})
	if _, err := b.CreateTopic("t", 8); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CreateTopic("t", 3); err != ErrExists {
		t.Fatalf("got %v", err)
	}
	if _, err := b.CreateTopic("t", 0); err != ErrPartitions {
		t.Fatalf("got %v", err)
	}
	where := map[string]int{}
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("k%d", i%10)
		p, _, err := b.Publish("t", -1, []byte(k), []byte("v"))
		if err != nil {
			t.Fatal(err)
		}
		if prev, ok := where[k]; ok && prev != p {
			t.Fatalf("key %s unstable: %d then %d", k, prev, p)
		}
		where[k] = p
	}
	if _, _, err := b.Publish("t", 8, nil, nil); err != ErrBadPartition {
		t.Fatalf("got %v", err)
	}
	b.Close()

	// Partition count and data survive a restart, and key->partition mapping is unchanged.
	b = open(t, dir, Config{})
	defer b.Close()
	ti := b.Topics()
	if len(ti) != 1 || len(ti[0].Partitions) != 8 {
		t.Fatalf("topics after restart: %+v", ti)
	}
	var total int64
	for _, p := range ti[0].Partitions {
		total += p.Next
	}
	if total != 100 {
		t.Fatalf("total messages %d", total)
	}
	for k, want := range where {
		if p, _, _ := b.Publish("t", -1, []byte(k), []byte("v")); p != want {
			t.Fatalf("key %s moved after restart: %d -> %d", k, want, p)
		}
	}
}

func TestUnkeyedRoundRobin(t *testing.T) {
	b := open(t, t.TempDir(), Config{DefaultPartitions: 3})
	defer b.Close()
	counts := map[int]int{}
	for i := 0; i < 30; i++ {
		p, _, _ := b.Publish("rr", -1, nil, []byte("x"))
		counts[p]++
	}
	if counts[0] != 10 || counts[1] != 10 || counts[2] != 10 {
		t.Fatalf("uneven: %v", counts)
	}
}

func TestDisableAutoCreate(t *testing.T) {
	b := open(t, t.TempDir(), Config{DisableAutoCreate: true})
	defer b.Close()
	if _, _, err := b.Publish("nope", -1, nil, []byte("x")); err != ErrNoTopic {
		t.Fatalf("got %v", err)
	}
	b.CreateTopic("yes", 1)
	if _, _, err := b.Publish("yes", -1, nil, []byte("x")); err != nil {
		t.Fatal(err)
	}
}

func TestOffsetStorePersistence(t *testing.T) {
	dir := t.TempDir()
	b := open(t, dir, Config{})
	b.Offsets().Commit("g", "t", 0, 5)
	b.Offsets().Commit("g", "t", 1, 9)
	b.Offsets().Commit("h", "t", 0, 1)
	if v, ok := b.Offsets().Get("g", "t", 1); !ok || v != 9 {
		t.Fatalf("get: %d %v", v, ok)
	}
	if _, ok := b.Offsets().Get("g", "t", 2); ok {
		t.Fatal("unexpected offset")
	}
	b.Close() // flushes

	b = open(t, dir, Config{})
	defer b.Close()
	snap := b.Offsets().Snapshot()
	want := []GroupOffset{{"g", "t", 0, 5}, {"g", "t", 1, 9}, {"h", "t", 0, 1}}
	if fmt.Sprint(snap) != fmt.Sprint(want) {
		t.Fatalf("snapshot %v want %v", snap, want)
	}
}

type recorder struct {
	mu   sync.Mutex
	last Assignment
}

func (r *recorder) notify(a Assignment) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if a.Gen > r.last.Gen {
		r.last = a
	}
}

func (r *recorder) get() Assignment {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

func TestGroupAssignment(t *testing.T) {
	b := open(t, t.TempDir(), Config{})
	defer b.Close()
	topic, _ := b.CreateTopic("t", 6)

	var a, c, d recorder
	leaveA := b.JoinGroup("g", topic, "a", a.notify)
	if got := a.get().Partitions; len(got) != 6 {
		t.Fatalf("sole member should own all: %v", got)
	}
	leaveB := b.JoinGroup("g", topic, "b", c.notify)
	leaveC := b.JoinGroup("g", topic, "c", d.notify)

	all := func() []int {
		var u []int
		for _, r := range []*recorder{&a, &c, &d} {
			u = append(u, r.get().Partitions...)
		}
		sort.Ints(u)
		return u
	}
	if fmt.Sprint(all()) != "[0 1 2 3 4 5]" {
		t.Fatalf("coverage with 3 members: %v", all())
	}
	for _, r := range []*recorder{&a, &c, &d} {
		if n := len(r.get().Partitions); n != 2 {
			t.Fatalf("uneven split: %d", n)
		}
	}
	leaveB()
	leaveB() // idempotent
	if len(a.get().Partitions)+len(d.get().Partitions) != 6 {
		t.Fatalf("after leave: a=%v d=%v", a.get(), d.get())
	}
	leaveA()
	leaveC()
	// The group is garbage-collected once empty; a new join starts fresh.
	b.mu.Lock()
	n := len(b.groups)
	b.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d groups leaked", n)
	}
	var e recorder
	leave := b.JoinGroup("g", topic, "e", e.notify)
	defer leave()
	if len(e.get().Partitions) != 6 {
		t.Fatalf("rejoin: %v", e.get())
	}
}

func TestGroupConcurrentChurn(t *testing.T) {
	b := open(t, t.TempDir(), Config{})
	defer b.Close()
	topic, _ := b.CreateTopic("t", 4)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var r recorder
			leave := b.JoinGroup("g", topic, fmt.Sprint(i), r.notify)
			time.Sleep(time.Millisecond)
			leave()
		}(i)
	}
	wg.Wait()
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.groups) != 0 {
		t.Fatalf("groups leaked after churn: %d", len(b.groups))
	}
}

func TestRetentionAcrossPartitions(t *testing.T) {
	b := open(t, t.TempDir(), Config{DefaultPartitions: 2,
		Log: commitlog.Options{SegmentBytes: 200, RetentionBytes: 400}})
	defer b.Close()
	for i := 0; i < 200; i++ {
		b.Publish("r", i%2, nil, []byte("0123456789"))
	}
	b.EnforceRetention()
	for _, p := range b.Topics()[0].Partitions {
		if p.Earliest == 0 {
			t.Fatalf("partition %d not trimmed: %+v", p.Partition, p)
		}
	}
}

func TestCommitZeroIsStoredAndPersisted(t *testing.T) {
	dir := t.TempDir()
	b := open(t, dir, Config{})
	if _, ok := b.Offsets().Get("g", "t", 0); ok {
		t.Fatal("fresh store should have no offset")
	}
	b.Offsets().Commit("g", "t", 0, 0)
	if v, ok := b.Offsets().Get("g", "t", 0); !ok || v != 0 {
		t.Fatalf("committed 0 not visible: %d %v", v, ok)
	}
	b.Close()
	b = open(t, dir, Config{})
	defer b.Close()
	if v, ok := b.Offsets().Get("g", "t", 0); !ok || v != 0 {
		t.Fatalf("committed 0 lost across restart: %d %v", v, ok)
	}
}

func TestPublishBatchRoutingAndReceipts(t *testing.T) {
	b := open(t, t.TempDir(), Config{DefaultPartitions: 4})
	defer b.Close()
	msgs := make([]Message, 50)
	for i := range msgs {
		msgs[i] = Message{Key: []byte(fmt.Sprintf("k%d", i%7)), Value: []byte(fmt.Sprint(i))}
	}
	rs, err := b.PublishBatch("batch", -1, msgs)
	if err != nil {
		t.Fatal(err)
	}
	topic, _ := b.Topic("batch", false)
	seen := map[Receipt]bool{}
	for i, r := range rs {
		if want := topic.Partition(msgs[i].Key); r.Partition != want {
			t.Fatalf("msg %d routed to %d, key hash says %d", i, r.Partition, want)
		}
		if seen[r] {
			t.Fatalf("duplicate receipt %+v", r)
		}
		seen[r] = true
		recs, err := topic.Parts[r.Partition].Read(r.Offset, 1, 1<<20)
		if err != nil || len(recs) != 1 || string(recs[0].Value) != string(msgs[i].Value) {
			t.Fatalf("receipt %+v does not point at msg %d: %v %v", r, i, recs, err)
		}
	}
	// Within one partition, batch order is preserved.
	for p := 0; p < 4; p++ {
		var last int64 = -1
		for i, r := range rs {
			if r.Partition == p {
				if r.Offset <= last {
					t.Fatalf("partition %d out of order at msg %d", p, i)
				}
				last = r.Offset
			}
		}
	}
	// Explicit partition: everything lands there, contiguously.
	rs, err = b.PublishBatch("batch", 2, msgs[:5])
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(rs); i++ {
		if rs[i].Partition != 2 || rs[i].Offset != rs[i-1].Offset+1 {
			t.Fatalf("not contiguous: %+v", rs)
		}
	}
	if _, err := b.PublishBatch("batch", 9, msgs); err != ErrBadPartition {
		t.Fatalf("got %v", err)
	}
	if rs, err := b.PublishBatch("batch", -1, nil); err != nil || len(rs) != 0 {
		t.Fatalf("empty batch: %v %v", rs, err)
	}
}

func TestPublishBatchTooLargeWritesNothing(t *testing.T) {
	b := open(t, t.TempDir(), Config{DefaultPartitions: 3, Log: commitlog.Options{MaxRecordBytes: 10}})
	defer b.Close()
	msgs := []Message{{Value: []byte("ok")}, {Key: []byte("a"), Value: []byte("fine")}, {Value: make([]byte, 11)}, {Value: []byte("ok")}}
	if _, err := b.PublishBatch("big", -1, msgs); err != commitlog.ErrRecordTooLarge {
		t.Fatalf("got %v", err)
	}
	for _, ti := range b.Topics() {
		for _, p := range ti.Partitions {
			if p.Next != 0 {
				t.Fatalf("partition %d has data after a rejected batch: %+v", p.Partition, p)
			}
		}
	}
}

func TestDeleteTopic(t *testing.T) {
	dir := t.TempDir()
	b := open(t, dir, Config{DefaultPartitions: 2})
	for i := 0; i < 10; i++ {
		b.Publish("doomed", -1, nil, []byte("x"))
	}
	b.Publish("survivor", -1, nil, []byte("y"))
	b.Offsets().Commit("g", "doomed", 0, 4)
	b.Offsets().Commit("g", "survivor", 0, 1)
	topic, _ := b.Topic("doomed", false)

	if err := b.DeleteTopic("doomed"); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteTopic("doomed"); err != ErrNoTopic {
		t.Fatalf("second delete: %v", err)
	}
	if err := b.DeleteTopic("../etc"); err != ErrInvalidTopic {
		t.Fatalf("got %v", err)
	}
	if _, err := os.Stat(dir + "/topics/doomed"); !os.IsNotExist(err) {
		t.Fatalf("topic dir still exists: %v", err)
	}
	if _, err := topic.Parts[0].Append(nil, nil); err == nil {
		t.Fatal("deleted topic's log still accepts appends")
	}
	if _, ok := b.Offsets().Get("g", "doomed", 0); ok {
		t.Fatal("offsets for deleted topic survived")
	}
	if v, ok := b.Offsets().Get("g", "survivor", 0); !ok || v != 1 {
		t.Fatalf("other topic's offsets disturbed: %d %v", v, ok)
	}
	// A topic with the same name starts over from scratch, also after a restart.
	p, off, err := b.Publish("doomed", -1, nil, []byte("reborn"))
	if err != nil || off != 0 {
		t.Fatalf("recreate: %d/%d %v", p, off, err)
	}
	b.Close()
	b = open(t, dir, Config{})
	defer b.Close()
	if len(b.Topics()) != 2 {
		t.Fatalf("topics after restart: %+v", b.Topics())
	}
	if _, ok := b.Offsets().Get("g", "doomed", 0); ok {
		t.Fatal("deleted offsets resurrected after restart")
	}
}

func TestResetOffsets(t *testing.T) {
	b := open(t, t.TempDir(), Config{DefaultPartitions: 2, Log: commitlog.Options{SegmentBytes: 100, RetentionBytes: 200}})
	defer b.Close()
	for i := 0; i < 60; i++ {
		b.Publish("r", i%2, nil, []byte("0123456789"))
	}
	b.EnforceRetention()
	topic, _ := b.Topic("r", false)
	e0, n0 := topic.Parts[0].Earliest(), topic.Parts[0].Next()
	if e0 == 0 {
		t.Fatal("retention did not trim")
	}

	offs, err := b.ResetOffsets("g", "r", -1, "earliest")
	if err != nil || offs[0] != e0 || offs[1] != topic.Parts[1].Earliest() {
		t.Fatalf("earliest: %v %v", offs, err)
	}
	if offs, err = b.ResetOffsets("g", "r", -1, "latest"); err != nil || offs[0] != n0 {
		t.Fatalf("latest: %v %v", offs, err)
	}
	// Numeric needs one partition and is clamped into the retained range.
	if _, err = b.ResetOffsets("g", "r", -1, "5"); err == nil {
		t.Fatal("numeric without partition should fail")
	}
	if offs, err = b.ResetOffsets("g", "r", 0, "0"); err != nil || offs[0] != e0 {
		t.Fatalf("clamp low: %v %v", offs, err)
	}
	if offs, err = b.ResetOffsets("g", "r", 0, "99999"); err != nil || offs[0] != n0 {
		t.Fatalf("clamp high: %v %v", offs, err)
	}
	if v, _ := b.Offsets().Get("g", "r", 0); v != n0 {
		t.Fatalf("not stored: %d", v)
	}
	for _, bad := range []string{"", "banana", "-1"} {
		if bad == "" {
			continue
		}
		if _, err := b.ResetOffsets("g", "r", 0, bad); err == nil {
			t.Fatalf("target %q accepted", bad)
		}
	}
	if _, err := b.ResetOffsets("g", "nope", -1, "earliest"); err != ErrNoTopic {
		t.Fatalf("got %v", err)
	}
	if _, err := b.ResetOffsets("g", "r", 7, "earliest"); err != ErrBadPartition {
		t.Fatalf("got %v", err)
	}
	if _, err := b.ResetOffsets("", "r", -1, "earliest"); err == nil {
		t.Fatal("empty group accepted")
	}
	// Refused while a member is live; allowed again once it leaves.
	var rec recorder
	leave := b.JoinGroup("g", topic, "m", rec.notify)
	if _, err := b.ResetOffsets("g", "r", -1, "earliest"); err != ErrGroupActive {
		t.Fatalf("got %v", err)
	}
	if _, err := b.ResetOffsets("other-group", "r", -1, "earliest"); err != nil {
		t.Fatalf("different group should be unaffected: %v", err)
	}
	leave()
	if _, err := b.ResetOffsets("g", "r", -1, "earliest"); err != nil {
		t.Fatal(err)
	}
}

func TestGroupsInfo(t *testing.T) {
	b := open(t, t.TempDir(), Config{DefaultPartitions: 2})
	defer b.Close()
	for i := 0; i < 10; i++ {
		b.Publish("gi", 0, nil, []byte("x"))
	}
	topic, _ := b.Topic("gi", false)
	b.Offsets().Commit("g1", "gi", 0, 4)
	b.Offsets().Commit("g1", "gi", 1, 0)
	var rec recorder
	leave := b.JoinGroup("g1", topic, "m", rec.notify)
	defer leave()
	infos := b.Groups()
	if len(infos) != 2 {
		t.Fatalf("%+v", infos)
	}
	if infos[0].Partition != 0 || infos[0].Committed != 4 || infos[0].Lag != 6 || infos[0].Members != 1 {
		t.Fatalf("p0: %+v", infos[0])
	}
	if infos[1].Lag != 0 || infos[1].Members != 1 {
		t.Fatalf("p1: %+v", infos[1])
	}
}

// Recreating a topic under the same name, with fewer partitions, while a
// member of the old incarnation's group lingers, must not hand the new
// topic's members partitions that do not exist.
func TestDeleteTopicResetsGroupsForRecreatedTopic(t *testing.T) {
	b := open(t, t.TempDir(), Config{})
	defer b.Close()
	old, _ := b.CreateTopic("t", 8)
	var stale recorder
	leaveStale := b.JoinGroup("g", old, "ghost", stale.notify)
	defer leaveStale() // the ghost never learned the topic died

	if err := b.DeleteTopic("t"); err != nil {
		t.Fatal(err)
	}
	fresh, _ := b.CreateTopic("t", 2)
	var rec recorder
	leave := b.JoinGroup("g", fresh, "new", rec.notify)
	defer leave()
	got := rec.get().Partitions
	if len(got) != 2 {
		t.Fatalf("new member assigned %v; the recreated topic has 2 partitions, and the ghost must not share its group", got)
	}
	for _, p := range got {
		if p >= fresh.NumPartitions() {
			t.Fatalf("assigned nonexistent partition %d", p)
		}
	}
	// The ghost's late leave must not disturb the new group either.
	leaveStale()
	if len(rec.get().Partitions) != 2 {
		t.Fatalf("ghost leaving changed the new group: %v", rec.get())
	}
}

func TestCommitIfCurrentIgnoresDeadTopics(t *testing.T) {
	b := open(t, t.TempDir(), Config{})
	defer b.Close()
	old, _ := b.CreateTopic("t", 1)
	if !b.CommitIfCurrent("g", old, 0, 5) {
		t.Fatal("commit on a live topic refused")
	}
	b.DeleteTopic("t")
	if b.CommitIfCurrent("g", old, 0, 9) {
		t.Fatal("commit accepted for a deleted topic")
	}
	if _, ok := b.Offsets().Get("g", "t", 0); ok {
		t.Fatal("deleted topic's offset resurrected by a late commit")
	}
	fresh, _ := b.CreateTopic("t", 1)
	if b.CommitIfCurrent("g", old, 0, 9) {
		t.Fatal("commit through a stale *Topic reached the recreated topic")
	}
	if !b.CommitIfCurrent("g", fresh, 0, 1) {
		t.Fatal("commit on the recreated topic refused")
	}
}

// Resets race with members joining and leaving. This is a stress test for the
// race detector and for deadlocks (the check-and-commit takes b.mu while
// JoinGroup registers under it): it asserts both outcomes occur, every call
// returns promptly with a legal result, and no group state leaks.
func TestResetOffsetsVersusJoinStress(t *testing.T) {
	b := open(t, t.TempDir(), Config{DefaultPartitions: 2})
	defer b.Close()
	topic, _ := b.CreateTopic("t", 2)
	for i := 0; i < 10; i++ {
		b.Publish("t", 0, nil, []byte("x"))
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				var r recorder
				leave := b.JoinGroup("g", topic, fmt.Sprint(w), r.notify)
				time.Sleep(200 * time.Microsecond)
				leave()
			}
		}(w)
	}
	var okResets, refused int
	for i := 0; i < 400; i++ {
		switch _, err := b.ResetOffsets("g", "t", -1, "earliest"); err {
		case nil:
			okResets++
		case ErrGroupActive:
			refused++
		default:
			t.Fatal(err)
		}
		time.Sleep(100 * time.Microsecond)
	}
	close(stop)
	wg.Wait()
	if okResets == 0 || refused == 0 {
		t.Fatalf("stress did not exercise both outcomes: ok=%d refused=%d", okResets, refused)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.groups) != 0 {
		t.Fatalf("groups leaked: %d", len(b.groups))
	}
}
