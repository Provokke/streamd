package commitlog

import (
	"testing"
	"time"
)

var payload = make([]byte, 256)

func BenchmarkAppendNoSync(b *testing.B) {
	l, _ := Open(b.TempDir(), Options{SyncInterval: -1})
	defer l.Close()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Append(nil, payload)
	}
}

func BenchmarkAppendBatch100NoSync(b *testing.B) {
	l, _ := Open(b.TempDir(), Options{SyncInterval: -1})
	defer l.Close()
	batch := make([]Record, 100)
	for i := range batch {
		batch[i].Value = payload
	}
	b.SetBytes(int64(len(payload) * len(batch)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.AppendBatch(batch)
	}
}

func BenchmarkAppendGroupSync(b *testing.B) {
	l, _ := Open(b.TempDir(), Options{SyncInterval: 5 * time.Millisecond})
	defer l.Close()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Append(nil, payload)
	}
}

func BenchmarkAppendFsyncEach(b *testing.B) {
	l, _ := Open(b.TempDir(), Options{})
	defer l.Close()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Append(nil, payload)
	}
}

func BenchmarkRead(b *testing.B) {
	l, _ := Open(b.TempDir(), Options{SegmentBytes: 8 << 20, SyncInterval: -1})
	defer l.Close()
	const n = 50000
	for i := 0; i < n; i++ {
		l.Append(nil, payload)
	}
	b.SetBytes(int64(len(payload)) * 256)
	b.ResetTimer()
	var off int64
	for i := 0; i < b.N; i++ {
		recs, _ := l.Read(off, 256, 1<<20)
		off += int64(len(recs))
		if off >= n {
			off = 0
		}
	}
}
