// Package server exposes a broker over TCP and manages client sockets:
// connection limits, auth, idle timeouts, bounded per-connection write queues,
// slow-consumer disconnects, credit-based flow control and graceful shutdown.
package server

import (
	"bufio"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Provokke/streamd/broker"
	"github.com/Provokke/streamd/commitlog"
	"github.com/Provokke/streamd/protocol"
)

// Config configures a Server. Zero values select defaults.
type Config struct {
	MaxConns      int           // default 10000
	IdleTimeout   time.Duration // close conns silent this long (default 60s; <0 disables)
	WriteTimeout  time.Duration // max time to flush to a client (default 10s)
	OutQueue      int           // buffered outbound frames per conn (default 1024)
	MaxFrameBytes int           // max inbound line (default 2 MiB)
	BatchRecords  int           // records read from the log per pump iteration (default 256)
	AuthToken     string        // if set, clients must auth first
	ReadOnly      bool          // reject publishes (followers)
	TLS           *tls.Config   // if set, clients must connect over TLS
	Logger        *slog.Logger
}

func (c *Config) defaults() {
	if c.MaxConns <= 0 {
		c.MaxConns = 10000
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 60 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 10 * time.Second
	}
	if c.OutQueue <= 0 {
		c.OutQueue = 1024
	}
	if c.MaxFrameBytes <= 0 {
		c.MaxFrameBytes = 2 << 20
	}
	if c.BatchRecords <= 0 {
		c.BatchRecords = 256
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type stats struct {
	conns, totalConns, rejectedConns, slowDisconnects, idleDisconnects atomic.Int64
	msgsIn, msgsOut, bytesIn, bytesOut, subs                           atomic.Int64
}

// maxBatch bounds messages per pubbatch (the frame size limit applies as well).
const maxBatch = 10000

// Server serves a Broker over TCP.
type Server struct {
	b   *broker.Broker
	cfg Config
	log *slog.Logger

	mu    sync.Mutex
	ln    net.Listener
	conns map[*conn]struct{}
	shut  bool
	wg    sync.WaitGroup
	st    stats
	subID atomic.Uint64
}

// New creates a server for b.
func New(b *broker.Broker, cfg Config) *Server {
	cfg.defaults()
	return &Server{b: b, cfg: cfg, log: cfg.Logger, conns: map[*conn]struct{}{}}
}

// Addr returns the listening address (after Serve has started).
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// ListenAndServe listens on addr and serves until Shutdown.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// ErrServerClosed is returned by Serve after Shutdown.
var ErrServerClosed = errors.New("server closed")

// Serve accepts connections on ln until Shutdown.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.shut {
		s.mu.Unlock()
		ln.Close()
		return ErrServerClosed
	}
	s.ln = ln
	s.mu.Unlock()
	s.log.Info("listening", "addr", ln.Addr().String())

	var delay time.Duration
	for {
		nc, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			shut := s.shut
			s.mu.Unlock()
			if shut {
				return ErrServerClosed
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				if delay == 0 {
					delay = 5 * time.Millisecond
				} else if delay *= 2; delay > time.Second {
					delay = time.Second
				}
				time.Sleep(delay)
				continue
			}
			return err
		}
		delay = 0
		if tc, ok := nc.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
			tc.SetKeepAlive(true)
			tc.SetKeepAlivePeriod(30 * time.Second)
		}
		s.accept(nc)
	}
}

func (s *Server) accept(nc net.Conn) {
	s.mu.Lock()
	if s.shut || len(s.conns) >= s.cfg.MaxConns {
		s.mu.Unlock()
		s.st.rejectedConns.Add(1)
		nc.SetWriteDeadline(time.Now().Add(time.Second))
		nc.Write([]byte(`{"op":"err","error":"too many connections"}` + "\n"))
		nc.Close()
		return
	}
	remote := nc.RemoteAddr().String()
	if s.cfg.TLS != nil {
		nc = tls.Server(nc, s.cfg.TLS) // handshake happens in readLoop, under a deadline
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &conn{s: s, nc: nc, out: make(chan []byte, s.cfg.OutQueue), ctx: ctx, cancel: cancel, finish: make(chan struct{}),
		subs: map[string]*subscription{}, authed: s.cfg.AuthToken == "", remote: remote}
	s.conns[c] = struct{}{}
	s.wg.Add(2)
	s.mu.Unlock()
	s.st.conns.Add(1)
	s.st.totalConns.Add(1)
	s.log.Debug("client connected", "remote", c.remote)
	go c.writeLoop()
	go c.readLoop()
}

// Shutdown stops accepting, closes all client sockets and waits for their
// goroutines (or ctx expiry). It does not close the broker.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.shut = true
	if s.ln != nil {
		s.ln.Close()
	}
	cs := make([]*conn, 0, len(s.conns))
	for c := range s.conns {
		cs = append(cs, c)
	}
	s.mu.Unlock()
	for _, c := range cs {
		c.close()
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats returns a snapshot of server counters.
func (s *Server) Stats() map[string]int64 {
	return map[string]int64{
		"connections":          s.st.conns.Load(),
		"connections_total":    s.st.totalConns.Load(),
		"connections_rejected": s.st.rejectedConns.Load(),
		"slow_disconnects":     s.st.slowDisconnects.Load(),
		"idle_disconnects":     s.st.idleDisconnects.Load(),
		"subscriptions":        s.st.subs.Load(),
		"messages_in":          s.st.msgsIn.Load(),
		"messages_out":         s.st.msgsOut.Load(),
		"bytes_in":             s.st.bytesIn.Load(),
		"bytes_out":            s.st.bytesOut.Load(),
	}
}

// ---- connection ----

type conn struct {
	s      *Server
	nc     net.Conn
	remote string
	out    chan []byte
	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once
	authed bool

	finish     chan struct{} // closed to flush queued frames, then close
	finishOnce sync.Once
	finishing  atomic.Bool

	mu   sync.Mutex
	subs map[string]*subscription
}

type partState struct {
	cancel   context.CancelFunc
	acked    int64 // highest acknowledged offset (start-1 initially); guarded by subscription.mu
	sentNext atomic.Int64

	// mu orders "revoke" against "about to send": the pump records a message
	// as sent (and checks revoked) under mu before enqueueing it, so a revoke
	// that computes the in-flight count always sees every message that will
	// ever be sent for this partition.
	mu      sync.Mutex
	revoked bool
}

type subscription struct {
	id      string
	topic   *broker.Topic
	group   string
	window  int
	credits chan struct{} // shared flow-control tokens across partitions
	ctx     context.Context
	cancel  context.CancelFunc

	mu    sync.Mutex
	leave func() // group leave (nil for ungrouped subs); guarded by mu
	gen   uint64
	parts map[int]*partState

	// Rebalance notifications from other connections only record the newest
	// assignment here and poke the applier, so they never block on this
	// connection's socket. mbMu guards pending; a stale generation can never
	// replace a newer one, whatever order notifications arrive in.
	mbMu    sync.Mutex
	pending *broker.Assignment
	wake    chan struct{}
}

func (c *conn) close() {
	c.once.Do(func() {
		c.cancel()
		c.nc.Close()
		c.mu.Lock()
		subs := c.subs
		c.subs = map[string]*subscription{}
		c.mu.Unlock()
		for _, sub := range subs {
			c.s.endSub(sub)
		}
		c.s.mu.Lock()
		delete(c.s.conns, c)
		c.s.mu.Unlock()
		c.s.st.conns.Add(-1)
		c.s.log.Debug("client disconnected", "remote", c.remote)
	})
}

func (s *Server) endSub(sub *subscription) {
	sub.cancel()
	sub.mu.Lock()
	leave := sub.leave
	sub.leave = nil
	sub.mu.Unlock()
	if leave != nil {
		leave() // triggers a rebalance for the remaining members
	}
	s.st.subs.Add(-1)
}

// send queues a frame. If the client cannot drain its queue within
// WriteTimeout it is considered slow and disconnected.
func (c *conn) send(f *protocol.Frame) bool {
	b, err := json.Marshal(f)
	if err != nil {
		return false
	}
	return c.enqueue(append(b, '\n'))
}

// enqueue queues one encoded, newline-terminated frame for the writer.
func (c *conn) enqueue(b []byte) bool {
	select {
	case c.out <- b:
		return true
	case <-c.ctx.Done():
		return false
	default:
	}
	t := time.NewTimer(c.s.cfg.WriteTimeout)
	defer t.Stop()
	select {
	case c.out <- b:
		return true
	case <-c.ctx.Done():
		return false
	case <-t.C:
		c.s.st.slowDisconnects.Add(1)
		c.s.log.Warn("disconnecting slow client", "remote", c.remote)
		c.close()
		return false
	}
}

func (c *conn) writeLoop() {
	defer c.s.wg.Done()
	defer c.close()
	w := bufio.NewWriterSize(c.nc, 64<<10)
	var n int64
	// write returns false on error, counting write timeouts as slow clients.
	write := func(b []byte) bool {
		if _, err := w.Write(b); err != nil {
			c.writeErr(err)
			return false
		}
		n += int64(len(b))
		return true
	}
	flush := func() bool {
		if err := w.Flush(); err != nil {
			c.writeErr(err)
			return false
		}
		c.s.st.bytesOut.Add(n)
		n = 0
		return true
	}
	drain := func() bool { // write everything already queued
		for {
			select {
			case b := <-c.out:
				if !write(b) {
					return false
				}
			default:
				return true
			}
		}
	}
	for {
		select {
		case b := <-c.out:
			c.nc.SetWriteDeadline(time.Now().Add(c.s.cfg.WriteTimeout))
			if !write(b) || !drain() || !flush() {
				return
			}
		case <-c.finish:
			c.nc.SetWriteDeadline(time.Now().Add(time.Second))
			drain()
			flush()
			return
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *conn) writeErr(err error) {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		c.s.st.slowDisconnects.Add(1)
		c.s.log.Warn("disconnecting slow client", "remote", c.remote)
	}
}

func (c *conn) readLoop() {
	defer c.s.wg.Done()
	defer func() {
		// After fail(), the writer flushes the final frame and closes.
		if !c.finishing.Load() {
			c.close()
		}
	}()
	if tc, ok := c.nc.(*tls.Conn); ok {
		c.nc.SetDeadline(time.Now().Add(10 * time.Second))
		if err := tc.Handshake(); err != nil {
			c.s.log.Debug("tls handshake failed", "remote", c.remote, "err", err)
			return
		}
		c.nc.SetDeadline(time.Time{})
	}
	sc := bufio.NewScanner(c.nc)
	sc.Buffer(make([]byte, 0, 4096), c.s.cfg.MaxFrameBytes)
	for {
		if c.s.cfg.IdleTimeout > 0 {
			c.nc.SetReadDeadline(time.Now().Add(c.s.cfg.IdleTimeout))
		}
		if !sc.Scan() {
			switch err := sc.Err(); {
			case errors.Is(err, bufio.ErrTooLong):
				c.fail(0, "frame too large")
			case err != nil:
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					c.s.st.idleDisconnects.Add(1)
					c.fail(0, "idle timeout")
				}
			}
			return
		}
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		c.s.st.bytesIn.Add(int64(len(line)) + 1)
		var f protocol.Frame
		if err := json.Unmarshal(line, &f); err != nil {
			if !c.reply(0, "malformed frame: "+err.Error()) {
				return
			}
			continue
		}
		if !c.authed && f.Op != protocol.OpAuth {
			c.fail(f.ID, "authentication required")
			return
		}
		if !c.handle(&f) {
			return
		}
	}
}

// fail sends a final error frame, then has the writer flush it and close.
func (c *conn) fail(id uint64, msg string) {
	c.finishing.Store(true)
	c.reply(id, msg)
	c.finishOnce.Do(func() { close(c.finish) })
}

func (c *conn) reply(id uint64, errMsg string) bool {
	return c.send(&protocol.Frame{Op: protocol.OpErr, ID: id, Error: errMsg})
}

func (c *conn) ok(id uint64) *protocol.Frame { return &protocol.Frame{Op: protocol.OpOK, ID: id} }

// handle processes one request; false means close the connection.
func (c *conn) handle(f *protocol.Frame) bool {
	s := c.s
	switch f.Op {
	case protocol.OpAuth:
		if s.cfg.AuthToken != "" && subtle.ConstantTimeCompare([]byte(f.Token), []byte(s.cfg.AuthToken)) != 1 {
			c.fail(f.ID, "invalid token")
			return false
		}
		c.authed = true
		return c.send(c.ok(f.ID))

	case protocol.OpPing:
		return c.send(&protocol.Frame{Op: protocol.OpPong, ID: f.ID})

	case protocol.OpCreate:
		if s.cfg.ReadOnly {
			return c.reply(f.ID, "read-only follower")
		}
		n := f.Partitions
		if n == 0 {
			n = 1
		}
		if _, err := s.b.CreateTopic(f.Topic, n); err != nil {
			return c.reply(f.ID, err.Error())
		}
		return c.send(c.ok(f.ID))

	case protocol.OpPub:
		if s.cfg.ReadOnly {
			return c.reply(f.ID, "read-only follower")
		}
		val, err := f.ValueBytes()
		if err != nil {
			return c.reply(f.ID, "bad value encoding")
		}
		key, err := f.KeyBytes()
		if err != nil {
			return c.reply(f.ID, "bad key encoding")
		}
		part := -1
		if f.Partition != nil {
			part = *f.Partition
			if part < 0 {
				return c.reply(f.ID, "invalid partition")
			}
		}
		part, off, err := s.b.Publish(f.Topic, part, key, val)
		if err != nil {
			return c.reply(f.ID, err.Error())
		}
		s.st.msgsIn.Add(1)
		r := c.ok(f.ID)
		r.Offset = protocol.Int64(off)
		r.Partition = protocol.Int(part)
		return c.send(r)

	case protocol.OpPubBatch:
		if s.cfg.ReadOnly {
			return c.reply(f.ID, "read-only follower")
		}
		if len(f.Msgs) == 0 {
			return c.reply(f.ID, "empty batch")
		}
		if len(f.Msgs) > maxBatch {
			return c.reply(f.ID, fmt.Sprintf("batch too large (max %d messages)", maxBatch))
		}
		part := -1
		if f.Partition != nil {
			if part = *f.Partition; part < 0 {
				return c.reply(f.ID, "invalid partition")
			}
		}
		msgs := make([]broker.Message, len(f.Msgs))
		for i := range f.Msgs {
			m := &f.Msgs[i]
			fr := protocol.Frame{Key: m.Key, KeyEnc: m.KeyEnc, Value: m.Value, Enc: m.Enc}
			k, kerr := fr.KeyBytes()
			v, verr := fr.ValueBytes()
			if kerr != nil || verr != nil {
				return c.reply(f.ID, fmt.Sprintf("message %d: bad encoding", i))
			}
			msgs[i] = broker.Message{Key: k, Value: v}
		}
		rcpts, err := s.b.PublishBatch(f.Topic, part, msgs)
		if err != nil {
			return c.reply(f.ID, err.Error())
		}
		s.st.msgsIn.Add(int64(len(msgs)))
		r := c.ok(f.ID)
		for _, rc := range rcpts {
			r.Receipts = append(r.Receipts, protocol.Receipt(rc))
		}
		return c.send(r)

	case protocol.OpDelete:
		if s.cfg.ReadOnly {
			return c.reply(f.ID, "read-only follower")
		}
		if err := s.b.DeleteTopic(f.Topic); err != nil {
			return c.reply(f.ID, err.Error())
		}
		return c.send(c.ok(f.ID))

	case protocol.OpReset:
		if s.cfg.ReadOnly {
			return c.reply(f.ID, "read-only follower")
		}
		part := -1
		if f.Partition != nil {
			part = *f.Partition
		}
		to := f.To
		if to == "" {
			to = "earliest"
		}
		offs, err := s.b.ResetOffsets(f.Group, f.Topic, part, to)
		if err != nil {
			return c.reply(f.ID, err.Error())
		}
		r := c.ok(f.ID)
		r.Resume = offs
		return c.send(r)

	case protocol.OpGroups:
		r := c.ok(f.ID)
		for _, g := range s.b.Groups() {
			if f.Topic != "" && g.Topic != f.Topic {
				continue
			}
			if f.Group != "" && g.Group != f.Group {
				continue
			}
			r.Groups = append(r.Groups, protocol.GroupInfo(g))
		}
		return c.send(r)

	case protocol.OpSub:
		if err := c.subscribe(f); err != nil {
			return c.reply(f.ID, err.Error())
		}
		return true // subscribe queued the ok itself, ahead of any message

	case protocol.OpAck:
		if err := c.ack(f); err != nil {
			return c.reply(0, err.Error())
		}
		return true

	case protocol.OpUnsub:
		c.mu.Lock()
		sub := c.subs[f.Sub]
		delete(c.subs, f.Sub)
		c.mu.Unlock()
		if sub != nil {
			s.endSub(sub)
		}
		return true

	case protocol.OpTopics:
		r := c.ok(f.ID)
		for _, t := range s.b.Topics() {
			ti := protocol.TopicInfo{Name: t.Name}
			for _, p := range t.Partitions {
				ti.Partitions = append(ti.Partitions, protocol.PartitionInfo(p))
			}
			r.Topics = append(r.Topics, ti)
		}
		return c.send(r)

	case protocol.OpStats:
		r := c.ok(f.ID)
		r.Stats = s.Stats()
		return c.send(r)

	default:
		return c.reply(f.ID, "unknown op "+strconv.Quote(f.Op))
	}
}

func (c *conn) subscribe(f *protocol.Frame) error {
	s := c.s
	id := f.Sub
	if id == "" {
		id = "s" + strconv.FormatUint(s.subID.Add(1), 10)
	}
	c.mu.Lock()
	_, dup := c.subs[id]
	c.mu.Unlock()
	if dup {
		return fmt.Errorf("subscription %q already exists", id)
	}
	t, err := s.b.Topic(f.Topic, s.b.AutoCreate())
	if err != nil {
		return err
	}
	from := f.From
	if from == "" {
		if f.Group != "" {
			from = "committed"
		} else {
			from = "latest"
		}
	}
	var fixedStart int64 = -1
	switch from {
	case "earliest", "latest":
	case "committed":
		if f.Group == "" {
			return errors.New(`from "committed" requires a group`)
		}
	default:
		n, err := strconv.ParseInt(from, 10, 64)
		if err != nil || n < 0 {
			return fmt.Errorf("invalid from %q", from)
		}
		if f.Group != "" {
			return errors.New("a numeric from cannot be combined with a group; use the committed offset")
		}
		if f.Partition == nil && t.NumPartitions() > 1 {
			return errors.New("a numeric from needs a partition")
		}
		fixedStart = n
	}
	if f.Partition != nil && (*f.Partition < 0 || *f.Partition >= t.NumPartitions()) {
		return broker.ErrBadPartition
	}

	window := min(max(f.Window, 0), 1<<20)
	ctx, cancel := context.WithCancel(c.ctx)
	sub := &subscription{id: id, topic: t, group: f.Group, window: window,
		ctx: ctx, cancel: cancel, parts: map[int]*partState{}}
	if window > 0 {
		sub.credits = make(chan struct{}, window)
		for i := 0; i < window; i++ {
			sub.credits <- struct{}{}
		}
	}

	// startPos decides where partition p begins streaming.
	startPos := func(p int) (int64, error) {
		lg := t.Parts[p]
		earliest, next := lg.Earliest(), lg.Next()
		if f.Group != "" {
			if v, ok := s.b.Offsets().Get(f.Group, t.Name, p); ok {
				return min(max(v, earliest), next), nil
			}
			start := earliest
			if from == "latest" {
				start = next
			}
			// Persist the initial position so a reconnect or rebalance before
			// the first ack resumes here instead of re-resolving "latest".
			s.b.CommitIfCurrent(f.Group, t, p, start)
			return start, nil
		}
		if v, ok := f.Resume[p]; ok {
			return min(max(v, earliest), next), nil
		}
		switch from {
		case "earliest":
			return earliest, nil
		case "latest":
			return next, nil
		}
		if fixedStart > next {
			return 0, fmt.Errorf("offset %d beyond log end %d", fixedStart, next)
		}
		return max(fixedStart, earliest), nil
	}

	c.mu.Lock()
	c.subs[id] = sub
	c.mu.Unlock()
	s.st.subs.Add(1)
	if c.ctx.Err() != nil {
		// The connection closed while we were setting up, possibly before
		// close() could see this subscription: end it here.
		c.dropSub(sub)
		return errors.New("connection closing")
	}

	r := c.ok(f.ID)
	r.Sub = id

	if f.Group == "" {
		var plist []int
		if f.Partition != nil {
			plist = []int{*f.Partition}
		} else {
			for p := 0; p < t.NumPartitions(); p++ {
				plist = append(plist, p)
			}
		}
		// Validate every start position before streaming anything.
		starts := map[int]int64{}
		for _, p := range plist {
			st, err := startPos(p)
			if err != nil {
				c.dropSub(sub)
				return err
			}
			starts[p] = st
		}
		r.Parts = plist
		r.Resume = starts
		if !c.send(r) { // ok must precede the first message
			return errors.New("connection closing")
		}
		sub.mu.Lock()
		for _, p := range plist {
			sub.startPartition(c, p, starts[p])
		}
		sub.mu.Unlock()
		return nil
	}

	if !c.send(r) {
		return errors.New("connection closing")
	}
	sub.wake = make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-sub.wake:
				sub.mbMu.Lock()
				a := sub.pending
				sub.pending = nil
				sub.mbMu.Unlock()
				if a != nil {
					sub.applyAssignment(c, *a, startPos)
				}
			case <-sub.ctx.Done():
				return
			}
		}
	}()
	leave := s.b.JoinGroup(f.Group, t, c.remote+"/"+id, sub.post)
	sub.mu.Lock()
	if sub.ctx.Err() != nil { // closed while joining: release the membership now
		sub.mu.Unlock()
		leave()
		return nil
	}
	sub.leave = leave
	sub.mu.Unlock()
	return nil
}

// post records an assignment for the subscription's applier and wakes it. An
// assignment older than one already waiting is dropped. It never blocks.
func (sub *subscription) post(a broker.Assignment) {
	sub.mbMu.Lock()
	if sub.pending == nil || a.Gen > sub.pending.Gen {
		sub.pending = &a
	}
	sub.mbMu.Unlock()
	select {
	case sub.wake <- struct{}{}:
	default: // a wake-up is already queued
	}
}

// dropSub removes a subscription that failed to start.
func (c *conn) dropSub(sub *subscription) {
	c.mu.Lock()
	_, ok := c.subs[sub.id]
	delete(c.subs, sub.id)
	c.mu.Unlock()
	if ok { // idempotent: several partition pumps may report the same failure
		c.s.endSub(sub)
	}
}

// applyAssignment moves the subscription to a new partition set: pumps for
// revoked partitions stop (their unacked messages may be redelivered to the
// new owner) and pumps for new partitions start at the committed offset.
func (sub *subscription) applyAssignment(c *conn, a broker.Assignment, startPos func(int) (int64, error)) {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	if a.Gen <= sub.gen || sub.ctx.Err() != nil {
		return
	}
	sub.gen = a.Gen
	keep := map[int]bool{}
	for _, p := range a.Partitions {
		keep[p] = true
	}
	for p, ps := range sub.parts {
		if !keep[p] {
			ps.cancel()
			ps.mu.Lock()
			ps.revoked = true
			inflight := ps.sentNext.Load() - 1 - ps.acked
			ps.mu.Unlock()
			// Messages in flight on a revoked partition will never be acked
			// by this member: give their flow-control credits back.
			sub.refund(inflight)
			delete(sub.parts, p)
		}
	}
	for _, p := range a.Partitions {
		if _, ok := sub.parts[p]; ok {
			continue
		}
		if p < 0 || p >= sub.topic.NumPartitions() {
			c.s.log.Error("assignment names a partition the topic does not have",
				"sub", sub.id, "topic", sub.topic.Name, "partition", p)
			continue
		}
		start, err := startPos(p)
		if err != nil {
			c.s.log.Error("start position", "sub", sub.id, "partition", p, "err", err)
			continue
		}
		sub.startPartition(c, p, start)
	}
	c.send(&protocol.Frame{Op: protocol.OpAssign, Sub: sub.id, Topic: sub.topic.Name, Parts: a.Partitions})
}

// refund returns n flow-control credits (never blocks, never exceeds window).
func (sub *subscription) refund(n int64) {
	for i := int64(0); i < n && sub.credits != nil; i++ {
		select {
		case sub.credits <- struct{}{}:
		default:
			return
		}
	}
}

// startPartition launches the pump for partition p. sub.mu must be held.
func (sub *subscription) startPartition(c *conn, p int, start int64) {
	pctx, cancel := context.WithCancel(sub.ctx)
	ps := &partState{cancel: cancel, acked: start - 1}
	ps.sentNext.Store(start)
	sub.parts[p] = ps
	go c.pump(sub, pctx, p, ps, start)
}

// pump streams one partition's records to the client, honouring flow-control
// credits, until its context is cancelled.
func (c *conn) pump(sub *subscription, ctx context.Context, part int, ps *partState, next int64) {
	lg := sub.topic.Parts[part]
	batch := c.s.cfg.BatchRecords
	for {
		notify := lg.Notify()
		recs, err := lg.Read(next, batch, 1<<20)
		if errors.Is(err, commitlog.ErrOffsetOutOfRange) {
			// Retention overtook us (or the log was reset): skip ahead.
			e := lg.Earliest()
			if e == next {
				e = lg.Next()
			}
			c.s.log.Warn("subscriber fell behind retention", "sub", sub.id,
				"topic", sub.topic.Name, "partition", part, "from", next, "to", e)
			next = e
			sub.mu.Lock()
			if sub.parts[part] != ps { // revoked meanwhile: that already refunded
				sub.mu.Unlock()
				return
			}
			// Skipped-over in-flight messages will never be acked: return
			// their credits before resetting the ack baseline.
			sub.refund(ps.sentNext.Load() - 1 - ps.acked)
			ps.acked = next - 1
			ps.sentNext.Store(next)
			sub.mu.Unlock()
			continue
		}
		if errors.Is(err, commitlog.ErrClosed) {
			// The topic was deleted (or the broker is closing) under us.
			c.send(&protocol.Frame{Op: protocol.OpErr, Sub: sub.id, Error: "topic deleted"})
			c.dropSub(sub) // releases the group membership and credits
			return
		}
		if err != nil {
			c.s.log.Error("read failed", "topic", sub.topic.Name, "partition", part, "err", err)
			c.send(&protocol.Frame{Op: protocol.OpErr, Sub: sub.id, Error: "read failed: " + err.Error()})
			return
		}
		if len(recs) == 0 {
			select {
			case <-notify:
			case <-ctx.Done():
				return
			}
			continue
		}
		for i := range recs {
			r := &recs[i]
			if sub.credits != nil {
				select {
				case <-sub.credits:
				case <-ctx.Done():
					return
				}
			}
			ps.mu.Lock()
			if ps.revoked || ctx.Err() != nil { // revoked while waiting for credit
				ps.mu.Unlock()
				sub.refund(1)
				return
			}
			next = r.Offset + 1
			ps.sentNext.Store(next) // counted as in flight before it can be acked
			ps.mu.Unlock()
			m := protocol.MsgFrame{Sub: sub.id, Topic: sub.topic.Name,
				Partition: part, Offset: r.Offset, TS: r.Timestamp}
			m.SetKey(r.Key)
			m.SetValue(r.Value)
			// Hand-encoded: this is the hottest path in the broker.
			buf := make([]byte, 0, 160+len(m.Key)+len(m.Value))
			buf = append(protocol.AppendMsg(buf, &m), '\n')
			if !c.enqueue(buf) {
				return
			}
			c.s.st.msgsOut.Add(1)
		}
	}
}

func (c *conn) ack(f *protocol.Frame) error {
	if f.Offset == nil {
		return errors.New("ack requires offset")
	}
	c.mu.Lock()
	sub := c.subs[f.Sub]
	c.mu.Unlock()
	if sub == nil {
		return fmt.Errorf("unknown subscription %q", f.Sub)
	}
	part := 0
	if f.Partition != nil {
		part = *f.Partition
	}
	off := *f.Offset
	sub.mu.Lock()
	defer sub.mu.Unlock()
	ps := sub.parts[part]
	if ps == nil {
		return nil // partition was revoked in a rebalance: stale ack, ignore
	}
	if off >= ps.sentNext.Load() {
		return fmt.Errorf("ack %d is beyond last delivered offset", off)
	}
	if off <= ps.acked {
		return nil // duplicate / stale cumulative ack
	}
	n := off - ps.acked
	ps.acked = off
	if sub.group != "" {
		c.s.b.CommitIfCurrent(sub.group, sub.topic, part, off+1)
	}
	sub.refund(n)
	return nil
}
