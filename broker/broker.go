// Package broker manages topics (one commit log each), consumer-group offsets
// and retention. It is transport-agnostic; package server exposes it over TCP.
package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Provokke/streamd/commitlog"
)

var (
	ErrInvalidTopic = errors.New("invalid topic name (allowed: letters, digits, '.', '_', '-'; max 128)")
	ErrNoTopic      = errors.New("topic does not exist")
	ErrClosed       = errors.New("broker closed")
	ErrPartitions   = errors.New("partition count must be between 1 and 1024")
	ErrBadPartition = errors.New("partition out of range")
	ErrExists       = errors.New("topic exists with a different partition count")
)

// MaxPartitions bounds partitions per topic.
const MaxPartitions = 1024

var topicRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// Config configures a Broker.
type Config struct {
	Dir               string
	Log               commitlog.Options
	RetentionInterval time.Duration // how often retention runs (default 30s)
	DisableAutoCreate bool          // if set, topics must be created explicitly
	DefaultPartitions int           // partitions for auto-created topics (default 1)
	Logger            *slog.Logger
}

// Topic is a named set of partitions, each an independent commit log.
// Ordering is guaranteed within a partition.
type Topic struct {
	Name  string
	Parts []*commitlog.Log
	dir   string
	rr    atomic.Uint32
}

// NumPartitions returns the partition count.
func (t *Topic) NumPartitions() int { return len(t.Parts) }

// Partition picks a partition for key: a stable hash for keyed messages,
// round-robin for unkeyed ones.
func (t *Topic) Partition(key []byte) int {
	n := len(t.Parts)
	if n == 1 {
		return 0
	}
	if len(key) == 0 {
		return int(t.rr.Add(1)-1) % n
	}
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32() % uint32(n))
}

// PartitionInfo describes one partition's retained range.
type PartitionInfo struct {
	Partition int   `json:"partition"`
	Earliest  int64 `json:"earliest"`
	Next      int64 `json:"next"`
	Bytes     int64 `json:"bytes"`
}

// TopicInfo describes a topic.
type TopicInfo struct {
	Name       string          `json:"name"`
	Partitions []PartitionInfo `json:"partitions"`
}

// Broker owns all topics.
type Broker struct {
	cfg     Config
	log     *slog.Logger
	mu      sync.RWMutex
	topics  map[string]*Topic
	offsets *OffsetStore
	groups  map[string]*group
	closed  bool
	stop    chan struct{}
	wg      sync.WaitGroup
}

// Open loads existing topics from disk and starts background retention.
func Open(cfg Config) (*Broker, error) {
	if cfg.Dir == "" {
		return nil, errors.New("broker: Dir required")
	}
	if cfg.RetentionInterval <= 0 {
		cfg.RetentionInterval = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.DefaultPartitions <= 0 {
		cfg.DefaultPartitions = 1
	}
	if err := os.MkdirAll(filepath.Join(cfg.Dir, "topics"), 0o755); err != nil {
		return nil, err
	}
	offs, err := openOffsetStore(filepath.Join(cfg.Dir, "offsets.json"))
	if err != nil {
		return nil, err
	}
	b := &Broker{cfg: cfg, log: cfg.Logger, topics: map[string]*Topic{}, offsets: offs,
		groups: map[string]*group{}, stop: make(chan struct{})}

	entries, err := os.ReadDir(filepath.Join(cfg.Dir, "topics"))
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() || !topicRe.MatchString(e.Name()) {
			continue
		}
		if _, err := b.openTopic(e.Name(), 1); err != nil {
			b.Close()
			return nil, fmt.Errorf("topic %q: %w", e.Name(), err)
		}
	}
	b.wg.Add(1)
	go b.retentionLoop()
	return b, nil
}

type topicMeta struct {
	Partitions int `json:"partitions"`
}

func (b *Broker) openTopic(name string, partitions int) (*Topic, error) {
	dir := filepath.Join(b.cfg.Dir, "topics", name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	metaPath := filepath.Join(dir, "meta.json")
	if raw, err := os.ReadFile(metaPath); err == nil {
		var m topicMeta
		if err := json.Unmarshal(raw, &m); err != nil || m.Partitions < 1 {
			return nil, fmt.Errorf("bad %s", metaPath)
		}
		partitions = m.Partitions
	} else if os.IsNotExist(err) {
		raw, _ := json.Marshal(topicMeta{partitions})
		if err := writeFileAtomic(metaPath, raw); err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}
	t := &Topic{Name: name, dir: dir}
	for i := 0; i < partitions; i++ {
		lg, err := commitlog.Open(filepath.Join(dir, fmt.Sprintf("p%04d", i)), b.cfg.Log)
		if err != nil {
			for _, o := range t.Parts {
				o.Close()
			}
			return nil, fmt.Errorf("partition %d: %w", i, err)
		}
		t.Parts = append(t.Parts, lg)
	}
	b.topics[name] = t
	return t, nil
}

func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	if serr := f.Sync(); werr == nil {
		werr = serr
	}
	f.Close()
	if werr != nil {
		return werr
	}
	return os.Rename(tmp, path)
}

// Topic returns a topic, creating it (with DefaultPartitions) when create is true.
func (b *Broker) Topic(name string, create bool) (*Topic, error) {
	if !create {
		return b.getTopic(name, 0)
	}
	return b.getTopic(name, b.cfg.DefaultPartitions)
}

// CreateTopic creates a topic with the given partition count. If it already
// exists with the same count it is returned; a different count is ErrExists.
func (b *Broker) CreateTopic(name string, partitions int) (*Topic, error) {
	if partitions < 1 || partitions > MaxPartitions {
		return nil, ErrPartitions
	}
	t, err := b.getTopic(name, partitions)
	if err != nil {
		return nil, err
	}
	if t.NumPartitions() != partitions {
		return nil, ErrExists
	}
	return t, nil
}

// getTopic looks a topic up; if create > 0 it creates it with that many partitions.
func (b *Broker) getTopic(name string, create int) (*Topic, error) {
	if !topicRe.MatchString(name) || name == "." || name == ".." {
		return nil, ErrInvalidTopic
	}
	b.mu.RLock()
	t, ok := b.topics[name]
	closed := b.closed
	b.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	if ok {
		return t, nil
	}
	if create == 0 {
		return nil, ErrNoTopic
	}
	if create < 1 || create > MaxPartitions {
		return nil, ErrPartitions
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	if t, ok := b.topics[name]; ok {
		return t, nil
	}
	return b.openTopic(name, create)
}

// AutoCreate reports whether publishing/subscribing may create topics.
func (b *Broker) AutoCreate() bool { return !b.cfg.DisableAutoCreate }

// Publish appends a message. partition < 0 selects by key. It returns the
// partition used and the message offset within it.
func (b *Broker) Publish(topic string, partition int, key, value []byte) (int, int64, error) {
	t, err := b.Topic(topic, b.AutoCreate())
	if err != nil {
		return 0, 0, err
	}
	if partition < 0 {
		partition = t.Partition(key)
	} else if partition >= t.NumPartitions() {
		return 0, 0, ErrBadPartition
	}
	off, err := t.Parts[partition].Append(key, value)
	return partition, off, err
}

// Message is one record to publish.
type Message struct{ Key, Value []byte }

// Receipt locates a published message.
type Receipt struct {
	Partition int
	Offset    int64
}

// PublishBatch appends msgs and returns a receipt per message, in order.
// partition < 0 routes each message by key. Messages bound for the same
// partition are appended atomically with a single fsync; the batch as a whole
// is not atomic across partitions, but all sizes are validated before anything
// is written, so a too-large message rejects the whole batch.
func (b *Broker) PublishBatch(topic string, partition int, msgs []Message) ([]Receipt, error) {
	t, err := b.Topic(topic, b.AutoCreate())
	if err != nil {
		return nil, err
	}
	if partition >= t.NumPartitions() {
		return nil, ErrBadPartition
	}
	byPart := make(map[int][]int) // partition -> indexes into msgs
	for i, m := range msgs {
		p := partition
		if p < 0 {
			p = t.Partition(m.Key)
		}
		if len(m.Key)+len(m.Value) > t.Parts[p].MaxRecordBytes() {
			return nil, commitlog.ErrRecordTooLarge
		}
		byPart[p] = append(byPart[p], i)
	}
	parts := make([]int, 0, len(byPart))
	for p := range byPart {
		parts = append(parts, p)
	}
	sort.Ints(parts)
	out := make([]Receipt, len(msgs))
	for _, p := range parts {
		idx := byPart[p]
		recs := make([]commitlog.Record, len(idx))
		for j, i := range idx {
			recs[j] = commitlog.Record{Key: msgs[i].Key, Value: msgs[i].Value}
		}
		first, err := t.Parts[p].AppendBatch(recs)
		if err != nil {
			return nil, err
		}
		for j, i := range idx {
			out[i] = Receipt{p, first + int64(j)}
		}
	}
	return out, nil
}

// DeleteTopic removes a topic, its data, and every group's offsets for it.
// Subscribers streaming it are ended by the server.
func (b *Broker) DeleteTopic(name string) error {
	if !topicRe.MatchString(name) {
		return ErrInvalidTopic
	}
	b.mu.Lock()
	defer b.mu.Unlock() // held throughout so a concurrent create can't race the removal
	if b.closed {
		return ErrClosed
	}
	t, ok := b.topics[name]
	if !ok {
		return ErrNoTopic
	}
	delete(b.topics, name)
	// Forget the topic's groups too. Surviving members hold the old group
	// object (their leave still works) but a topic recreated under the same
	// name, possibly with fewer partitions, must start with a clean group.
	for key := range b.groups {
		if strings.HasSuffix(key, "\x00"+name) {
			delete(b.groups, key)
		}
	}
	for _, lg := range t.Parts {
		lg.Close()
	}
	if err := os.RemoveAll(t.dir); err != nil {
		return err
	}
	b.offsets.DeleteTopic(name)
	return nil
}

// GroupInfo is one partition's state for a consumer group.
type GroupInfo struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Committed int64  `json:"committed"`
	Lag       int64  `json:"lag"`
	Members   int    `json:"members"`
}

// Groups describes every committed group position with its current lag and
// the number of live members.
func (b *Broker) Groups() []GroupInfo {
	var out []GroupInfo
	for _, g := range b.offsets.Snapshot() {
		b.mu.RLock()
		t := b.topics[g.Topic]
		members := 0
		if gr := b.groups[g.Group+"\x00"+g.Topic]; gr != nil {
			gr.mu.Lock()
			members = len(gr.members)
			gr.mu.Unlock()
		}
		b.mu.RUnlock()
		if t == nil || g.Partition >= t.NumPartitions() {
			continue
		}
		lag := max(t.Parts[g.Partition].Next()-g.Next, 0)
		out = append(out, GroupInfo{g.Group, g.Topic, g.Partition, g.Next, lag, members})
	}
	return out
}

// ErrGroupActive is returned when an offset reset targets a group with live members.
var ErrGroupActive = errors.New("group has active members; stop them before resetting offsets")

// ResetOffsets moves a group's committed positions. to is "earliest",
// "latest", or a decimal offset (which needs a single partition, selected with
// partition >= 0). Positions are clamped to the retained range. The group must
// have no live members. It returns the new next-offset per partition.
func (b *Broker) ResetOffsets(group, topic string, partition int, to string) (map[int]int64, error) {
	t, err := b.Topic(topic, false)
	if err != nil {
		return nil, err
	}
	if group == "" {
		return nil, errors.New("group required")
	}
	var parts []int
	if partition >= 0 {
		if partition >= t.NumPartitions() {
			return nil, ErrBadPartition
		}
		parts = []int{partition}
	} else {
		for i := range t.Parts {
			parts = append(parts, i)
		}
	}
	var fixed int64
	switch to {
	case "earliest", "latest":
	default:
		fixed, err = strconv.ParseInt(to, 10, 64)
		if err != nil || fixed < 0 {
			return nil, fmt.Errorf("invalid target %q (want earliest, latest or an offset)", to)
		}
		if len(parts) != 1 {
			return nil, errors.New("a numeric target needs a single partition")
		}
	}
	// Check for live members and commit under one lock: JoinGroup registers
	// members under b.mu, so nobody can join between the check and the commit,
	// and DeleteTopic cannot interleave either.
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.topics[topic] != t {
		return nil, ErrNoTopic
	}
	if gr := b.groups[group+"\x00"+topic]; gr != nil {
		gr.mu.Lock()
		active := len(gr.members) > 0
		gr.mu.Unlock()
		if active {
			return nil, ErrGroupActive
		}
	}
	out := make(map[int]int64, len(parts))
	for _, p := range parts {
		lg := t.Parts[p]
		var v int64
		switch to {
		case "earliest":
			v = lg.Earliest()
		case "latest":
			v = lg.Next()
		default:
			v = min(max(fixed, lg.Earliest()), lg.Next())
		}
		b.offsets.Commit(group, topic, p, v)
		out[p] = v
	}
	return out, nil
}

// Topics lists all topics sorted by name.
func (b *Broker) Topics() []TopicInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]TopicInfo, 0, len(b.topics))
	for _, t := range b.topics {
		ti := TopicInfo{Name: t.Name}
		for i, lg := range t.Parts {
			ti.Partitions = append(ti.Partitions, PartitionInfo{i, lg.Earliest(), lg.Next(), lg.Size()})
		}
		out = append(out, ti)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// CommitIfCurrent records a group's position only while t is still the live
// topic of that name, so a late ack for a deleted (or deleted and recreated)
// topic cannot resurrect stale offsets. It reports whether the commit happened.
func (b *Broker) CommitIfCurrent(group string, t *Topic, partition int, next int64) bool {
	b.mu.RLock() // DeleteTopic holds the write lock, so check and commit are atomic against it
	defer b.mu.RUnlock()
	if b.topics[t.Name] != t {
		return false
	}
	b.offsets.Commit(group, t.Name, partition, next)
	return true
}

// Offsets exposes the consumer-group offset store.
func (b *Broker) Offsets() *OffsetStore { return b.offsets }

func (b *Broker) retentionLoop() {
	defer b.wg.Done()
	t := time.NewTicker(b.cfg.RetentionInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			b.EnforceRetention()
		case <-b.stop:
			return
		}
	}
}

// EnforceRetention applies retention policy to every topic now.
func (b *Broker) EnforceRetention() {
	b.mu.RLock()
	ts := make([]*Topic, 0, len(b.topics))
	for _, t := range b.topics {
		ts = append(ts, t)
	}
	b.mu.RUnlock()
	for _, t := range ts {
		for i, lg := range t.Parts {
			n, err := lg.EnforceRetention(time.Now())
			if err != nil {
				b.log.Error("retention failed", "topic", t.Name, "partition", i, "err", err)
			} else if n > 0 {
				b.log.Info("retention removed segments", "topic", t.Name, "partition", i, "segments", n)
			}
		}
	}
}

// Close stops background work, flushes offsets and closes all logs.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.stop)
	b.mu.Unlock()
	b.wg.Wait()

	var first error
	b.mu.Lock()
	for _, t := range b.topics {
		for _, lg := range t.Parts {
			if err := lg.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	b.mu.Unlock()
	if err := b.offsets.Close(); err != nil && first == nil {
		first = err
	}
	return first
}

// OffsetStore persists committed consumer-group offsets (next offset to read).
// Writes are batched: commits are durable within flushInterval or on Close.
type OffsetStore struct {
	path  string
	mu    sync.Mutex
	data  map[string]map[string]int64 // group -> topic -> offset
	dirty bool
	stop  chan struct{}
	wg    sync.WaitGroup
	once  sync.Once
}

const flushInterval = 200 * time.Millisecond

func openOffsetStore(path string) (*OffsetStore, error) {
	s := &OffsetStore{path: path, data: map[string]map[string]int64{}, stop: make(chan struct{})}
	b, err := os.ReadFile(path)
	if err == nil {
		if err := json.Unmarshal(b, &s.data); err != nil {
			return nil, fmt.Errorf("offsets file %s: %w", path, err)
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(flushInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				_ = s.Flush()
			case <-s.stop:
				return
			}
		}
	}()
	return s, nil
}

func offKey(topic string, partition int) string { return fmt.Sprintf("%s:%d", topic, partition) }

// Get returns the committed offset for group/topic/partition.
func (s *OffsetStore) Get(group, topic string, partition int) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[group][offKey(topic, partition)]
	return v, ok
}

// Commit records the next offset the group wants to read.
func (s *OffsetStore) Commit(group, topic string, partition int, next int64) {
	k := offKey(topic, partition)
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.data[group]
	if m == nil {
		m = map[string]int64{}
		s.data[group] = m
	}
	if old, ok := m[k]; !ok || old != next { // a missing key is not "offset 0"
		m[k] = next
		s.dirty = true
	}
}

// DeleteTopic forgets every group's committed offsets for a topic.
func (s *OffsetStore) DeleteTopic(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := topic + ":"
	for g, m := range s.data {
		for k := range m {
			if strings.HasPrefix(k, prefix) {
				delete(m, k)
				s.dirty = true
			}
		}
		if len(m) == 0 {
			delete(s.data, g)
		}
	}
}

// GroupOffset is one committed position of a consumer group.
type GroupOffset struct {
	Group     string
	Topic     string
	Partition int
	Next      int64 // next offset the group will read
}

// Snapshot returns all committed group offsets, sorted.
func (s *OffsetStore) Snapshot() []GroupOffset {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []GroupOffset
	for g, m := range s.data {
		for k, v := range m {
			i := strings.LastIndexByte(k, ':')
			if i < 0 {
				continue
			}
			p, err := strconv.Atoi(k[i+1:])
			if err != nil {
				continue
			}
			out = append(out, GroupOffset{g, k[:i], p, v})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Group != b.Group {
			return a.Group < b.Group
		}
		if a.Topic != b.Topic {
			return a.Topic < b.Topic
		}
		return a.Partition < b.Partition
	})
	return out
}

// Flush atomically writes offsets to disk if changed.
func (s *OffsetStore) Flush() error {
	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return nil
	}
	b, err := json.Marshal(s.data)
	s.dirty = false
	s.mu.Unlock()
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		s.markDirty()
		return err
	}
	_, werr := f.Write(b)
	if serr := f.Sync(); werr == nil {
		werr = serr
	}
	f.Close()
	if werr != nil {
		s.markDirty()
		return werr
	}
	if err := os.Rename(tmp, s.path); err != nil {
		s.markDirty()
		return err
	}
	return nil
}

func (s *OffsetStore) markDirty() {
	s.mu.Lock()
	s.dirty = true
	s.mu.Unlock()
}

// Close stops the flusher and writes any pending offsets.
func (s *OffsetStore) Close() error {
	s.once.Do(func() { close(s.stop) })
	s.wg.Wait()
	return s.Flush()
}
