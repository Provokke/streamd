package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func open(t *testing.T, dir string, o Options) *Log {
	t.Helper()
	l, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestAppendReadAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentBytes: 512, IndexBytes: 64, SyncInterval: -1})
	defer l.Close()
	const n = 200
	for i := 0; i < n; i++ {
		off, err := l.Append([]byte("k"), []byte(fmt.Sprintf("value-%d", i)))
		if err != nil || off != int64(i) {
			t.Fatalf("append %d: off=%d err=%v", i, off, err)
		}
	}
	if len(l.segs) < 3 {
		t.Fatalf("expected multiple segments, got %d", len(l.segs))
	}
	for _, start := range []int64{0, 1, 77, 199} {
		recs, err := l.Read(start, 1000, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(recs)) != n-start {
			t.Fatalf("start %d: got %d records", start, len(recs))
		}
		for i, r := range recs {
			if r.Offset != start+int64(i) || string(r.Value) != fmt.Sprintf("value-%d", start+int64(i)) {
				t.Fatalf("bad record %+v", r)
			}
		}
	}
	if recs, err := l.Read(n, 10, 1<<20); err != nil || len(recs) != 0 {
		t.Fatalf("tail read: %v %v", recs, err)
	}
	if _, err := l.Read(n+1, 10, 1<<20); err != ErrOffsetOutOfRange {
		t.Fatalf("want out of range, got %v", err)
	}
	if recs, _ := l.Read(10, 5, 1<<20); len(recs) != 5 {
		t.Fatalf("maxRecords not honoured: %d", len(recs))
	}
}

func TestReopenAndTornTailRecovery(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentBytes: 1 << 20})
	for i := 0; i < 10; i++ {
		l.Append(nil, []byte(fmt.Sprintf("m%d", i)))
	}
	l.Close()

	// Simulate a crash mid-write: append half a record of garbage.
	p := segPath(dir, 0)
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{0, 0, 0, 40, 1, 2, 3, 4, 5})
	f.Close()

	l = open(t, dir, Options{SegmentBytes: 1 << 20})
	defer l.Close()
	if l.Next() != 10 {
		t.Fatalf("next=%d want 10", l.Next())
	}
	off, err := l.Append(nil, []byte("after"))
	if err != nil || off != 10 {
		t.Fatalf("append after recovery: %d %v", off, err)
	}
	recs, _ := l.Read(9, 10, 1<<20)
	if len(recs) != 2 || string(recs[1].Value) != "after" {
		t.Fatalf("unexpected %+v", recs)
	}
}

func TestCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentBytes: 128})
	for i := 0; i < 30; i++ {
		l.Append(nil, []byte("0123456789"))
	}
	l.Close()
	// Flip a byte in the oldest (closed) segment: must fail loudly.
	p := segPath(dir, 0)
	b, _ := os.ReadFile(p)
	b[20] ^= 0xff
	os.WriteFile(p, b, 0o644)
	if _, err := Open(dir, Options{SegmentBytes: 128}); err == nil {
		t.Fatal("expected corruption error")
	}
}

func TestRetentionBySizeAndAge(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentBytes: 256, RetentionBytes: 1024, SyncInterval: -1})
	defer l.Close()
	for i := 0; i < 200; i++ {
		l.Append(nil, []byte("0123456789"))
	}
	before := l.Earliest()
	n, err := l.EnforceRetention(time.Now())
	if err != nil || n == 0 {
		t.Fatalf("removed=%d err=%v", n, err)
	}
	if l.Earliest() <= before || l.Size() > 1024+256 {
		t.Fatalf("earliest=%d size=%d", l.Earliest(), l.Size())
	}
	if _, err := l.Read(0, 1, 100); err != ErrOffsetOutOfRange {
		t.Fatalf("want out of range, got %v", err)
	}
	if recs, err := l.Read(l.Earliest(), 5, 1<<20); err != nil || len(recs) != 5 {
		t.Fatalf("read from earliest: %v %v", len(recs), err)
	}

	// Age: everything but the active segment is "old" a day from now.
	l.opt.RetentionBytes = 0
	l.opt.RetentionAge = time.Hour
	l.EnforceRetention(time.Now().Add(24 * time.Hour))
	if len(l.segs) != 1 {
		t.Fatalf("active segment must survive, segs=%d", len(l.segs))
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	if len(files) != 1 {
		t.Fatalf("files on disk: %v", files)
	}
}

func TestReplicateAndReset(t *testing.T) {
	l := open(t, t.TempDir(), Options{})
	defer l.Close()
	if err := l.Replicate([]Record{{Offset: 5}}); err != ErrOffsetMismatch {
		t.Fatalf("want mismatch, got %v", err)
	}
	if err := l.Reset(100); err != nil {
		t.Fatal(err)
	}
	if err := l.Replicate([]Record{{Offset: 100, Timestamp: 42, Value: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	recs, err := l.Read(100, 10, 100)
	if err != nil || len(recs) != 1 || recs[0].Timestamp != 42 {
		t.Fatalf("%+v %v", recs, err)
	}
}

func TestNotifyAndTooLarge(t *testing.T) {
	l := open(t, t.TempDir(), Options{MaxRecordBytes: 8})
	defer l.Close()
	ch := l.Notify()
	if _, err := l.Append(nil, make([]byte, 9)); err != ErrRecordTooLarge {
		t.Fatalf("got %v", err)
	}
	select {
	case <-ch:
		t.Fatal("notified without append")
	default:
	}
	l.Append(nil, []byte("ok"))
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("no notification")
	}
}

func TestConcurrentAppendRead(t *testing.T) {
	l := open(t, t.TempDir(), Options{SegmentBytes: 4096, SyncInterval: -1})
	defer l.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			l.Append(nil, []byte("x"))
		}
	}()
	var next int64
	for next < 2000 {
		ch := l.Notify()
		recs, err := l.Read(next, 100, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			if r.Offset != next {
				t.Fatalf("gap: %d != %d", r.Offset, next)
			}
			next++
		}
		if len(recs) == 0 {
			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				t.Fatal("stalled")
			}
		}
	}
	<-done
}
