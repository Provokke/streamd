package commitlog

import (
	"fmt"
	"os"
	"testing"
)

// FuzzRecover feeds arbitrary bytes as a segment file. Open must never panic;
// when it succeeds, everything it reports must be readable and dense, and the
// log must accept new appends.
func FuzzRecover(f *testing.F) {
	// Seed with a valid segment plus mutations.
	dir := f.TempDir()
	l, _ := Open(dir, Options{SyncInterval: -1})
	for i := 0; i < 5; i++ {
		l.Append([]byte("k"), []byte(fmt.Sprintf("value-%d", i)))
	}
	l.Close()
	valid, _ := os.ReadFile(segPath(dir, 0))
	f.Add(valid)
	f.Add(valid[:len(valid)-3])
	f.Add(append(append([]byte{}, valid...), 0xde, 0xad))
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		d := t.TempDir()
		if err := os.WriteFile(segPath(d, 0), data, 0o644); err != nil {
			t.Fatal(err)
		}
		l, err := Open(d, Options{SyncInterval: -1})
		if err != nil {
			return // refusing to open is acceptable; panicking is not
		}
		defer l.Close()
		n := l.Next()
		recs, err := l.Read(0, int(n)+1, 1<<30)
		if err != nil {
			t.Fatalf("read after successful open: %v", err)
		}
		if int64(len(recs)) != n {
			t.Fatalf("next=%d but read %d records", n, len(recs))
		}
		for i, r := range recs {
			if r.Offset != int64(i) {
				t.Fatalf("non-dense offsets: %d at %d", r.Offset, i)
			}
		}
		off, err := l.Append(nil, []byte("after"))
		if err != nil || off != n {
			t.Fatalf("append after recovery: %d %v (want %d)", off, err, n)
		}
	})
}
