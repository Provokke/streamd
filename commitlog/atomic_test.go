package commitlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func logState(t *testing.T, l *Log) string {
	t.Helper()
	recs, err := l.Read(l.Earliest(), 1<<20, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	s := fmt.Sprintf("earliest=%d next=%d size=%d segs=%d:", l.Earliest(), l.Next(), l.Size(), len(l.segs))
	for _, r := range recs {
		s += fmt.Sprintf(" %d=%s", r.Offset, r.Value)
	}
	return s
}

func diskFiles(t *testing.T, dir string) []string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	return m
}

// A batch that fails partway — including after rolling into new segments —
// must leave no trace: same records, same segment files, still appendable.
func TestFailedBatchIsFullyRolledBack(t *testing.T) {
	for failAt := 0; failAt < 12; failAt++ {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			dir := t.TempDir()
			l, err := Open(dir, Options{SegmentBytes: 120, SyncInterval: -1})
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			for i := 0; i < 5; i++ {
				l.Append(nil, []byte(fmt.Sprint("pre", i)))
			}
			before, files := logState(t, l), diskFiles(t, dir)

			boom := errors.New("injected write failure")
			l.writeHook = func(i int) error {
				if i == failAt {
					return boom
				}
				return nil
			}
			batch := make([]Record, 12)
			for i := range batch {
				batch[i] = Record{Value: []byte(fmt.Sprint("batch-", i, "-padding-padding"))}
			}
			if _, err := l.AppendBatch(batch); err != boom {
				t.Fatalf("got %v", err)
			}
			if got := logState(t, l); got != before {
				t.Fatalf("log changed after failed batch:\n before %s\n after  %s", before, got)
			}
			if got := diskFiles(t, dir); fmt.Sprint(got) != fmt.Sprint(files) {
				t.Fatalf("segment files changed: %v -> %v", files, got)
			}
			// Still fully usable, offsets contiguous, and it survives a reopen.
			l.writeHook = nil
			off, err := l.Append(nil, []byte("post"))
			if err != nil || off != 5 {
				t.Fatalf("append after rollback: %d %v", off, err)
			}
			want := logState(t, l)
			l.Close()
			l2, err := Open(dir, Options{SegmentBytes: 120, SyncInterval: -1})
			if err != nil {
				t.Fatal(err)
			}
			defer l2.Close()
			if got := logState(t, l2); got != want {
				t.Fatalf("reopen differs:\n want %s\n got  %s", want, got)
			}
		})
	}
}

func TestReplicateValidatesWholeBatchUpFront(t *testing.T) {
	l, _ := Open(t.TempDir(), Options{SyncInterval: -1})
	defer l.Close()
	l.Append(nil, []byte("a"))
	// Second record has a gap: nothing may be written, not even the first.
	err := l.Replicate([]Record{{Offset: 1, Value: []byte("b")}, {Offset: 3, Value: []byte("d")}})
	if err != ErrOffsetMismatch {
		t.Fatalf("got %v", err)
	}
	if l.Next() != 1 {
		t.Fatalf("partial replicate leaked: next=%d", l.Next())
	}
}

func TestResetKeepsLogUsableAndSameBaseReusable(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Options{SegmentBytes: 100, SyncInterval: -1})
	defer l.Close()
	for i := 0; i < 20; i++ {
		l.Append(nil, []byte("xxxxxxxxxx"))
	}
	if err := l.Reset(0); err != nil { // same base as the first deleted segment
		t.Fatal(err)
	}
	if l.Next() != 0 || l.Earliest() != 0 || len(diskFiles(t, dir)) != 1 {
		t.Fatalf("after reset: next=%d earliest=%d files=%v", l.Next(), l.Earliest(), diskFiles(t, dir))
	}
	if off, err := l.Append(nil, []byte("y")); err != nil || off != 0 {
		t.Fatalf("append: %d %v", off, err)
	}
}

func TestMonotonicTimestampsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Options{SyncInterval: -1})
	far := int64(4102444800000) // year 2100
	if err := l.Replicate([]Record{{Offset: 0, Timestamp: far, Value: []byte("future")}}); err != nil {
		t.Fatal(err)
	}
	l.Close()
	l, _ = Open(dir, Options{SyncInterval: -1})
	defer l.Close()
	l.Append(nil, []byte("now"))
	recs, _ := l.Read(0, 10, 1<<20)
	if recs[1].Timestamp < far {
		t.Fatalf("timestamp went backwards after reopen: %d < %d", recs[1].Timestamp, far)
	}
}

func TestOperationsAfterCloseFailCleanly(t *testing.T) {
	l, _ := Open(t.TempDir(), Options{})
	l.Append(nil, []byte("x"))
	l.Close()
	if _, err := l.Append(nil, nil); err != ErrClosed {
		t.Fatalf("append: %v", err)
	}
	if _, err := l.Read(0, 1, 1); err != ErrClosed {
		t.Fatalf("read: %v", err)
	}
	if err := l.Sync(); err != ErrClosed {
		t.Fatalf("sync: %v", err)
	}
	if err := l.Reset(5); err != ErrClosed {
		t.Fatalf("reset: %v", err)
	}
	if err := l.Close(); err != nil { // idempotent
		t.Fatal(err)
	}
	_ = os.Stdout
}

func TestBackgroundFlusherSyncsAndStops(t *testing.T) {
	l, err := Open(t.TempDir(), Options{SyncInterval: 150 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	l.Append(nil, []byte("x"))
	l.mu.RLock()
	dirty := l.dirty
	l.mu.RUnlock()
	if !dirty {
		t.Fatal("append with a sync interval should leave the log dirty until the flusher runs")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		l.mu.RLock()
		dirty = l.dirty
		l.mu.RUnlock()
		if !dirty {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("flusher never synced")
		}
		time.Sleep(5 * time.Millisecond)
	}
	done := make(chan struct{})
	go func() { l.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung waiting for the flusher")
	}
}

func TestSyncModes(t *testing.T) {
	for _, iv := range []time.Duration{0, -1, 20 * time.Millisecond} {
		l, _ := Open(t.TempDir(), Options{SyncInterval: iv})
		l.Append(nil, []byte("a"))
		if err := l.Sync(); err != nil {
			t.Fatalf("interval %v: %v", iv, err)
		}
		l.mu.RLock()
		dirty := l.dirty
		l.mu.RUnlock()
		if dirty {
			t.Fatalf("interval %v: still dirty after Sync", iv)
		}
		l.Close()
	}
}
