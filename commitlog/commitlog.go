// Package commitlog implements a segmented, append-only, CRC-protected log.
//
// On-disk layout: a directory of segment files named <baseOffset>.log
// (zero-padded). Each record is
//
//	| payloadLen u32 | crc32c(payload) u32 | payload |
//	payload = | offset i64 | timestampMs i64 | keyLen u32 | key | value |
//
// All integers are big endian. Offsets are dense and monotonically increasing.
// On open the newest segment is scanned and any torn/corrupt tail is truncated
// (crash recovery). Corruption in an older segment is reported as an error.
package commitlog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrOffsetOutOfRange = errors.New("commitlog: offset out of range")
	ErrClosed           = errors.New("commitlog: closed")
	ErrRecordTooLarge   = errors.New("commitlog: record too large")
	ErrOffsetMismatch   = errors.New("commitlog: replicated offset does not match log end")

	errCorrupt = errors.New("commitlog: corrupt record")
	errTorn    = errors.New("commitlog: torn record")
)

const (
	headerSize  = 8
	fixedFields = 8 + 8 + 4
	suffix      = ".log"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Record is a single log entry.
type Record struct {
	Offset    int64
	Timestamp int64 // unix milliseconds
	Key       []byte
	Value     []byte
}

// Options configures a Log. Zero values select sensible defaults.
type Options struct {
	SegmentBytes   int64         // roll the active segment past this size (default 64 MiB)
	MaxRecordBytes int           // max len(key)+len(value) (default 1 MiB)
	IndexBytes     int64         // sparse index granularity (default 4 KiB)
	SyncInterval   time.Duration // 0: fsync every append; >0: background fsync; <0: never
	RetentionAge   time.Duration // delete closed segments whose newest record is older (0 = off)
	RetentionBytes int64         // delete oldest closed segments while total size exceeds (0 = off)
}

func (o Options) withDefaults() Options {
	if o.SegmentBytes <= 0 {
		o.SegmentBytes = 64 << 20
	}
	if o.MaxRecordBytes <= 0 {
		o.MaxRecordBytes = 1 << 20
	}
	if o.IndexBytes <= 0 {
		o.IndexBytes = 4 << 10
	}
	return o
}

type indexEntry struct {
	offset int64
	pos    int64
}

type segment struct {
	base   int64
	path   string
	f      *os.File
	size   int64
	next   int64
	lastTS int64
	index  []indexEntry // sparse; index[0] is always {base, 0}
	lastIx int64        // file position of the last index entry
}

// Log is a single-partition commit log. It is safe for concurrent use.
type Log struct {
	dir string
	opt Options

	mu     sync.RWMutex
	segs   []*segment
	next   int64
	lastTS int64 // newest timestamp in the log; assigned timestamps never go below it
	notify chan struct{}
	closed bool
	dirty  bool

	stop chan struct{}
	wg   sync.WaitGroup

	writeHook func(i int) error // tests only: fail the i-th write of a batch
}

// Open opens (creating if needed) the log in dir and recovers from a crash.
func Open(dir string, opt Options) (*Log, error) {
	opt = opt.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []int64
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, suffix) {
			continue
		}
		b, err := strconv.ParseInt(strings.TrimSuffix(name, suffix), 10, 64)
		if err != nil {
			continue
		}
		bases = append(bases, b)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })

	l := &Log{dir: dir, opt: opt, notify: make(chan struct{}), stop: make(chan struct{})}
	for i, b := range bases {
		s, err := openSegment(dir, b, opt, i == len(bases)-1)
		if err != nil {
			l.closeFiles()
			return nil, err
		}
		l.segs = append(l.segs, s)
	}
	if len(l.segs) == 0 {
		s, err := createSegment(dir, 0, opt)
		if err != nil {
			return nil, err
		}
		l.segs = append(l.segs, s)
	}
	l.next = l.segs[len(l.segs)-1].next
	for i := len(l.segs) - 1; i >= 0; i-- { // newest non-empty segment
		if l.segs[i].next > l.segs[i].base {
			l.lastTS = l.segs[i].lastTS
			break
		}
	}

	if opt.SyncInterval > 0 {
		l.wg.Add(1)
		go l.flusher()
	}
	return l, nil
}

func segPath(dir string, base int64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d%s", base, suffix))
}

func createSegment(dir string, base int64, opt Options) (*segment, error) {
	p := segPath(dir, base)
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	syncDir(dir)
	return &segment{base: base, path: p, f: f, next: base, index: []indexEntry{{base, 0}}}, nil
}

func openSegment(dir string, base int64, opt Options, last bool) (*segment, error) {
	p := segPath(dir, base)
	f, err := os.OpenFile(p, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	s := &segment{base: base, path: p, f: f, next: base, index: []indexEntry{{base, 0}}}
	r := bufio.NewReaderSize(io.NewSectionReader(f, 0, st.Size()), 1<<16)
	var pos int64
	for {
		rec, n, err := readRecord(r, st.Size()-pos)
		if err == io.EOF {
			break
		}
		if err == nil && rec.Offset != s.next {
			err = errCorrupt
		}
		if err != nil {
			if !last {
				f.Close()
				return nil, fmt.Errorf("%s: %w at byte %d", p, err, pos)
			}
			// Crash recovery: drop the torn tail of the newest segment.
			if terr := f.Truncate(pos); terr != nil {
				f.Close()
				return nil, terr
			}
			break
		}
		s.noteIndex(rec.Offset, pos, opt.IndexBytes)
		s.lastTS = rec.Timestamp
		pos += int64(n)
		s.next++
	}
	s.size = pos
	return s, nil
}

func (s *segment) noteIndex(offset, pos, every int64) {
	if pos-s.lastIx >= every {
		s.index = append(s.index, indexEntry{offset, pos})
		s.lastIx = pos
	}
}

// seek returns the index entry at or before target.
func (s *segment) seek(target int64) indexEntry {
	i := sort.Search(len(s.index), func(i int) bool { return s.index[i].offset > target })
	if i == 0 {
		return s.index[0]
	}
	return s.index[i-1]
}

func encode(r Record) []byte {
	plen := fixedFields + len(r.Key) + len(r.Value)
	buf := make([]byte, headerSize+plen)
	p := buf[headerSize:]
	binary.BigEndian.PutUint64(p[0:], uint64(r.Offset))
	binary.BigEndian.PutUint64(p[8:], uint64(r.Timestamp))
	binary.BigEndian.PutUint32(p[16:], uint32(len(r.Key)))
	copy(p[20:], r.Key)
	copy(p[20+len(r.Key):], r.Value)
	binary.BigEndian.PutUint32(buf[0:], uint32(plen))
	binary.BigEndian.PutUint32(buf[4:], crc32.Checksum(p, castagnoli))
	return buf
}

// readRecord decodes one record. remaining is the number of bytes available
// (including this record's header) and bounds allocations on corrupt lengths.
func readRecord(r io.Reader, remaining int64) (Record, int, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.EOF {
			return Record{}, 0, io.EOF
		}
		return Record{}, 0, errTorn
	}
	plen := int64(binary.BigEndian.Uint32(hdr[0:4]))
	sum := binary.BigEndian.Uint32(hdr[4:8])
	if plen < fixedFields || plen > remaining-headerSize {
		return Record{}, 0, errCorrupt
	}
	buf := make([]byte, plen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Record{}, 0, errTorn
	}
	if crc32.Checksum(buf, castagnoli) != sum {
		return Record{}, 0, errCorrupt
	}
	klen := int64(binary.BigEndian.Uint32(buf[16:20]))
	if fixedFields+klen > plen {
		return Record{}, 0, errCorrupt
	}
	return Record{
		Offset:    int64(binary.BigEndian.Uint64(buf[0:8])),
		Timestamp: int64(binary.BigEndian.Uint64(buf[8:16])),
		Key:       buf[20 : 20+klen : 20+klen],
		Value:     buf[20+klen:],
	}, headerSize + int(plen), nil
}

// Append writes one record and returns its offset.
func (l *Log) Append(key, value []byte) (int64, error) {
	return l.AppendBatch([]Record{{Key: key, Value: value}})
}

// AppendBatch appends records (assigning offsets and timestamps) and returns
// the offset of the first. With SyncInterval == 0 the batch shares one fsync.
func (l *Log) AppendBatch(recs []Record) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appendLocked(recs, false)
}

// Replicate appends records preserving their offsets and timestamps. The first
// record must be exactly at the log end. Used by followers.
func (l *Log) Replicate(recs []Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := l.appendLocked(recs, true)
	return err
}

// snapshot captures the mutable state an append can change, so a failed batch
// can be undone completely.
type snapshot struct {
	nsegs                      int
	size, next, lastTS, lastIx int64
	nidx                       int
	logNext, logLastTS         int64
}

func (l *Log) snapshotLocked() snapshot {
	a := l.segs[len(l.segs)-1]
	return snapshot{len(l.segs), a.size, a.next, a.lastTS, a.lastIx, len(a.index), l.next, l.lastTS}
}

// rollbackLocked restores sn: segments created since are deleted and the
// previously-active segment is truncated back to its old size.
func (l *Log) rollbackLocked(sn snapshot) {
	for len(l.segs) > sn.nsegs {
		s := l.segs[len(l.segs)-1]
		s.f.Close()
		os.Remove(s.path)
		l.segs = l.segs[:len(l.segs)-1]
	}
	a := l.segs[sn.nsegs-1]
	_ = a.f.Truncate(sn.size)
	a.size, a.next, a.lastTS, a.lastIx = sn.size, sn.next, sn.lastTS, sn.lastIx
	a.index = a.index[:sn.nidx]
	l.next, l.lastTS = sn.logNext, sn.logLastTS
}

// appendLocked appends recs atomically: either all become visible or, on any
// error, the log is left exactly as it was.
func (l *Log) appendLocked(recs []Record, preserve bool) (int64, error) {
	if l.closed {
		return 0, ErrClosed
	}
	if len(recs) == 0 {
		return l.next, nil
	}
	for i, r := range recs {
		if len(r.Key)+len(r.Value) > l.opt.MaxRecordBytes {
			return 0, ErrRecordTooLarge
		}
		if preserve && r.Offset != l.next+int64(i) {
			return l.next, ErrOffsetMismatch
		}
	}
	first := l.next
	sn := l.snapshotLocked()
	// Timestamps are monotonic even if the wall clock steps back or the log was
	// replicated from a node whose clock ran ahead.
	now := max(time.Now().UnixMilli(), l.lastTS)
	fail := func(err error) (int64, error) {
		l.rollbackLocked(sn)
		return first, err
	}
	for i := range recs {
		r := recs[i]
		if !preserve {
			r.Offset = l.next
			r.Timestamp = now
		}
		buf := encode(r)
		active := l.segs[len(l.segs)-1]
		if active.size > 0 && active.size+int64(len(buf)) > l.opt.SegmentBytes {
			if err := l.rollLocked(); err != nil {
				return fail(err)
			}
			active = l.segs[len(l.segs)-1]
		}
		if l.writeHook != nil {
			if err := l.writeHook(i); err != nil {
				return fail(err)
			}
		}
		if _, err := active.f.WriteAt(buf, active.size); err != nil {
			return fail(err)
		}
		active.noteIndex(r.Offset, active.size, l.opt.IndexBytes)
		active.size += int64(len(buf))
		active.next++
		active.lastTS = r.Timestamp
		l.lastTS = max(l.lastTS, r.Timestamp)
		l.next++
	}
	if l.opt.SyncInterval == 0 {
		l.dirty = true
		if err := l.syncLocked(); err != nil {
			return fail(err)
		}
	} else {
		l.dirty = true
	}
	close(l.notify)
	l.notify = make(chan struct{})
	return first, nil
}

func (l *Log) rollLocked() error {
	old := l.segs[len(l.segs)-1]
	if err := old.f.Sync(); err != nil {
		return err
	}
	s, err := createSegment(l.dir, l.next, l.opt)
	if err != nil {
		return err
	}
	l.segs = append(l.segs, s)
	return nil
}

func (l *Log) syncLocked() error {
	if !l.dirty {
		return nil
	}
	l.dirty = false
	return l.segs[len(l.segs)-1].f.Sync()
}

// Sync flushes the active segment to stable storage.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	return l.syncLocked()
}

func (l *Log) flusher() {
	defer l.wg.Done()
	t := time.NewTicker(l.opt.SyncInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			_ = l.Sync()
		case <-l.stop:
			return
		}
	}
}

// Read returns up to maxRecords records (and roughly maxBytes of payload)
// starting at offset. It returns (nil, nil) when offset is the log end.
func (l *Log) Read(offset int64, maxRecords, maxBytes int) ([]Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil, ErrClosed
	}
	if offset < l.segs[0].base || offset > l.next {
		return nil, ErrOffsetOutOfRange
	}
	if offset == l.next || maxRecords <= 0 {
		return nil, nil
	}
	si := sort.Search(len(l.segs), func(i int) bool { return l.segs[i].base > offset }) - 1
	var out []Record
	total := 0
outer:
	for ; si < len(l.segs); si++ {
		s := l.segs[si]
		ent := s.seek(offset)
		pos := ent.pos
		r := readerPool.Get().(*bufio.Reader)
		r.Reset(io.NewSectionReader(s.f, pos, s.size-pos))
		for {
			rec, n, err := readRecord(r, s.size-pos)
			if err == io.EOF {
				break
			}
			if err != nil {
				putReader(r)
				return out, fmt.Errorf("%s: %w at byte %d", s.path, err, pos)
			}
			pos += int64(n)
			if rec.Offset < offset {
				continue
			}
			out = append(out, rec)
			total += len(rec.Key) + len(rec.Value)
			if len(out) >= maxRecords || total >= maxBytes {
				putReader(r)
				break outer
			}
		}
		putReader(r)
	}
	return out, nil
}

// readerPool recycles the 32 KiB read buffers: tail readers issue one small
// Read per appended record, and allocating a fresh buffer each time dominated
// their cost.
var readerPool = sync.Pool{New: func() any { return bufio.NewReaderSize(nil, 32<<10) }}

func putReader(r *bufio.Reader) {
	r.Reset(nil) // drop the reference to the segment file
	readerPool.Put(r)
}

// Notify returns a channel closed on the next append (or Reset/Close). Obtain
// it *before* reading to avoid missing wakeups.
func (l *Log) Notify() <-chan struct{} {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.notify
}

// MaxRecordBytes is the largest key+value size Append accepts.
func (l *Log) MaxRecordBytes() int { return l.opt.MaxRecordBytes }

// Earliest is the oldest retained offset.
func (l *Log) Earliest() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.segs) == 0 {
		return l.next
	}
	return l.segs[0].base
}

// Next is the offset the next appended record will receive.
func (l *Log) Next() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.next
}

// Size is the total bytes on disk across segments.
func (l *Log) Size() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.sizeLocked()
}

func (l *Log) sizeLocked() int64 {
	var n int64
	for _, s := range l.segs {
		n += s.size
	}
	return n
}

// EnforceRetention deletes whole closed segments that violate the age or size
// policy. The active segment is never deleted. It returns the number removed.
func (l *Log) EnforceRetention(now time.Time) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	removed := 0
	for len(l.segs) > 1 {
		s := l.segs[0]
		drop := l.opt.RetentionAge > 0 && now.UnixMilli()-s.lastTS > l.opt.RetentionAge.Milliseconds()
		if l.opt.RetentionBytes > 0 && l.sizeLocked() > l.opt.RetentionBytes {
			drop = true
		}
		if !drop {
			break
		}
		if err := os.Remove(s.path); err != nil {
			return removed, err // segment stays open and in use
		}
		s.f.Close()
		l.segs = l.segs[1:]
		removed++
	}
	if removed > 0 {
		syncDir(l.dir)
	}
	return removed, nil
}

// Reset discards all data and restarts the log at base. Used by followers when
// the leader's retained range no longer overlaps the local log.
func (l *Log) Reset(base int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	// Delete oldest first: if a removal fails the remaining segments are still
	// a valid, contiguous suffix of the log.
	for len(l.segs) > 0 {
		s := l.segs[0]
		if err := os.Remove(s.path); err != nil {
			return err
		}
		s.f.Close()
		l.segs = l.segs[1:]
	}
	s, err := createSegment(l.dir, base, l.opt)
	if err != nil {
		// Nothing left to serve: fail closed instead of panicking later.
		l.closed = true
		close(l.stop)
		close(l.notify)
		return err
	}
	l.segs = []*segment{s}
	l.next = base
	l.dirty = false
	close(l.notify)
	l.notify = make(chan struct{})
	return nil
}

// Close flushes and closes the log, waking any waiters.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	close(l.stop)
	var err error
	if len(l.segs) > 0 {
		err = l.segs[len(l.segs)-1].f.Sync()
	}
	l.closeFiles()
	close(l.notify)
	l.mu.Unlock()
	l.wg.Wait()
	return err
}

func (l *Log) closeFiles() {
	for _, s := range l.segs {
		s.f.Close()
	}
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
}
