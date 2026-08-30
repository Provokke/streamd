package commitlog

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

// modelRec is the reference model's view of one retained record.
type modelRec struct {
	off      int64
	key, val []byte
}

type model struct {
	recs []modelRec // retained, dense offsets
	base int64      // offset of recs[0] (== earliest)
	next int64
}

func (m *model) dropBefore(off int64) {
	i := 0
	for i < len(m.recs) && m.recs[i].off < off {
		i++
	}
	m.recs = m.recs[i:]
	m.base = off
}

// TestModelRandomOps drives the log with random operations and compares every
// observable against a trivially-correct in-memory model.
func TestModelRandomOps(t *testing.T) {
	seeds := 40
	if testing.Short() {
		seeds = 8
	}
	if v, err := strconv.Atoi(os.Getenv("MODEL_SEEDS")); err == nil {
		seeds = v // e.g. MODEL_SEEDS=2000 for a long soak
	}
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runModel(t, int64(seed)) })
	}
}

func runModel(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	dir := t.TempDir()
	opt := Options{
		SegmentBytes: int64(150 + rng.Intn(500)),
		IndexBytes:   int64(1 + rng.Intn(120)),
		SyncInterval: -1,
	}
	if rng.Intn(2) == 0 {
		opt.RetentionBytes = int64(300 + rng.Intn(1500))
	}
	l, err := Open(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { l.Close() }()
	m := &model{}
	var trace []string
	fail := func(format string, a ...any) {
		t.Helper()
		t.Fatalf("seed %d: %s\nops: %v", seed, fmt.Sprintf(format, a...), trace)
	}
	randBytes := func() []byte {
		n := rng.Intn(40)
		if n == 0 {
			return nil
		}
		b := make([]byte, n)
		rng.Read(b)
		return b
	}
	check := func() {
		t.Helper()
		if l.Next() != m.next {
			fail("Next=%d want %d", l.Next(), m.next)
		}
		if l.Earliest() != m.base {
			fail("Earliest=%d want %d", l.Earliest(), m.base)
		}
		got, err := l.Read(l.Earliest(), 1<<20, 1<<30)
		if err != nil {
			fail("full read: %v", err)
		}
		if len(got) != len(m.recs) {
			fail("full read returned %d records want %d", len(got), len(m.recs))
		}
		var prevTS int64
		for i, r := range got {
			w := m.recs[i]
			if r.Offset != w.off || !bytes.Equal(r.Key, w.key) || !bytes.Equal(r.Value, w.val) {
				fail("record %d mismatch: got %+v want %+v", i, r, w)
			}
			if r.Timestamp < prevTS {
				fail("timestamps went backwards at %d", i)
			}
			prevTS = r.Timestamp
		}
	}
	reopen := func() {
		if err := l.Close(); err != nil {
			fail("close: %v", err)
		}
		if l, err = Open(dir, opt); err != nil {
			fail("reopen: %v", err)
		}
	}

	for step := 0; step < 150; step++ {
		switch op := rng.Intn(100); {
		case op < 35: // single append
			k, v := randBytes(), randBytes()
			off, err := l.Append(k, v)
			if err != nil || off != m.next {
				fail("append: off=%d err=%v want %d", off, err, m.next)
			}
			m.recs = append(m.recs, modelRec{off, k, v})
			m.next++
			trace = append(trace, "A")
		case op < 50: // batch append
			n := 1 + rng.Intn(6)
			batch := make([]Record, n)
			for i := range batch {
				batch[i] = Record{Key: randBytes(), Value: randBytes()}
			}
			first, err := l.AppendBatch(batch)
			if err != nil || first != m.next {
				fail("batch: first=%d err=%v want %d", first, err, m.next)
			}
			for _, r := range batch {
				m.recs = append(m.recs, modelRec{m.next, r.Key, r.Value})
				m.next++
			}
			trace = append(trace, fmt.Sprintf("B%d", n))
		case op < 75: // random range read
			from := m.base - 1 + rng.Int63n(m.next-m.base+3)
			max := 1 + rng.Intn(10)
			got, err := l.Read(from, max, 1<<30)
			switch {
			case from < m.base || from > m.next:
				if err != ErrOffsetOutOfRange {
					fail("read(%d) err=%v want out of range", from, err)
				}
			case err != nil:
				fail("read(%d): %v", from, err)
			default:
				want := m.recs[from-m.base:]
				if len(want) > max {
					want = want[:max]
				}
				if len(got) != len(want) {
					fail("read(%d,%d) len=%d want %d", from, max, len(got), len(want))
				}
				for i := range got {
					if got[i].Offset != want[i].off || !bytes.Equal(got[i].Value, want[i].val) {
						fail("read(%d) rec %d mismatch", from, i)
					}
				}
			}
			trace = append(trace, "R")
		case op < 85: // reopen
			reopen()
			trace = append(trace, "O")
		case op < 90: // crash mid-write: garbage tail on the active segment
			l.Close()
			files := segFiles(t, dir)
			f, err := os.OpenFile(files[len(files)-1], os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				fail("%v", err)
			}
			junk := make([]byte, 1+rng.Intn(60))
			rng.Read(junk)
			f.Write(junk)
			f.Close()
			if l, err = Open(dir, opt); err != nil {
				fail("recovery open: %v", err)
			}
			trace = append(trace, "T")
		case op < 96: // retention
			before := l.Earliest()
			if _, err := l.EnforceRetention(time.Now()); err != nil {
				fail("retention: %v", err)
			}
			e := l.Earliest()
			if e < before || e > m.next {
				fail("retention moved earliest %d -> %d (next %d)", before, e, m.next)
			}
			m.dropBefore(e)
			if opt.RetentionBytes > 0 && l.Size() > opt.RetentionBytes {
				// Only the active segment may remain over budget.
				if len(l.segs) != 1 {
					fail("size %d over budget %d with %d segments", l.Size(), opt.RetentionBytes, len(l.segs))
				}
			}
			trace = append(trace, "X")
		case op < 98: // replicate a few records at the log end
			n := 1 + rng.Intn(3)
			recs := make([]Record, n)
			for i := range recs {
				recs[i] = Record{Offset: m.next + int64(i), Timestamp: time.Now().UnixMilli() + 1000, Key: randBytes(), Value: randBytes()}
			}
			if err := l.Replicate(recs); err != nil {
				fail("replicate: %v", err)
			}
			for _, r := range recs {
				m.recs = append(m.recs, modelRec{r.Offset, r.Key, r.Value})
				m.next++
			}
			// A gap must be rejected and leave the log untouched.
			if err := l.Replicate([]Record{{Offset: m.next + 3}}); err != ErrOffsetMismatch {
				fail("gap replicate err=%v", err)
			}
			trace = append(trace, "P")
		default: // reset to a new base
			base := m.next + int64(rng.Intn(50))
			if err := l.Reset(base); err != nil {
				fail("reset: %v", err)
			}
			m.recs, m.base, m.next = nil, base, base
			trace = append(trace, "Z")
		}
		check()
	}
	// Final: a cold reopen reproduces the model exactly.
	reopen()
	check()
}

func segFiles(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, dir+"/"+e.Name())
	}
	return out
}
