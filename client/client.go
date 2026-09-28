// Package client is a Go client for streamd. One Client multiplexes requests
// and subscriptions over a single TCP connection.
//
// A slow subscription consumer never blocks the connection's read loop: each
// subscription has its own queue. Set SubscribeOptions.Window to bound that
// queue (the server stops sending once Window messages are unacknowledged).
//
// With WithReconnect the client survives connection loss: it redials with
// backoff, re-authenticates and re-establishes every subscription, then
// resumes where it left off. Group subscriptions resume from the group's
// committed offsets (so unacknowledged messages are redelivered: at-least-once);
// ungrouped subscriptions resume after the last message handed to the consumer.
// Idempotent requests (Ping, Topics, Stats, Groups, CreateTopic) are retried
// transparently across a reconnect. Others (Publish, PublishBatch, DeleteTopic,
// ResetOffsets) fail with ErrDisconnected if the connection drops mid-request and
// may be retried by the caller (a retried Publish can duplicate).
package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Provokke/streamd/protocol"
)

var (
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("client closed")
	// ErrStaleAck is returned by Subscription.Ack for a message delivered before
	// the connection was re-established; the ack is not sent. (Group
	// subscriptions redeliver such a message from the committed offset.)
	ErrStaleAck = errors.New("connection re-established since this message was delivered; ack not sent")
	// ErrDisconnected is returned for requests that were in flight (or issued
	// without waiting) while the connection was down and being re-established.
	ErrDisconnected = errors.New("connection lost; reconnecting")
)

// Message is a delivered record. Offsets are per partition.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Timestamp time.Time
	Key       string
	Value     []byte

	epoch uint64 // subscription epoch it was delivered in; see Subscription.Ack
}

// ServerError is an error the server returned for a request, as opposed to a
// connection problem. Use errors.As to tell them apart.
type ServerError string

func (e ServerError) Error() string { return string(e) }

// TopicInfo describes a topic and each partition's retained offset range.
type (
	TopicInfo     = protocol.TopicInfo
	PartitionInfo = protocol.PartitionInfo
)

// Option configures Dial.
type Option func(*options)

type options struct {
	token        string
	ping         time.Duration
	tls          *tls.Config
	reconnect    bool
	maxBackoff   time.Duration
	onDisconnect func(error)
	onReconnect  func()
}

// WithToken authenticates with the server's shared token.
func WithToken(t string) Option { return func(o *options) { o.token = t } }

// WithTLS connects over TLS using cfg (ServerName defaults to the dial host).
func WithTLS(cfg *tls.Config) Option { return func(o *options) { o.tls = cfg } }

// WithPingInterval sends keepalive pings (default 15s; must be below the
// server's idle timeout). Zero or negative disables.
func WithPingInterval(d time.Duration) Option { return func(o *options) { o.ping = d } }

// WithReconnect enables automatic reconnection with exponential backoff up to
// maxBackoff (default 10s if zero).
func WithReconnect(maxBackoff time.Duration) Option {
	return func(o *options) {
		o.reconnect = true
		if maxBackoff <= 0 {
			maxBackoff = 10 * time.Second
		}
		o.maxBackoff = maxBackoff
	}
}

// WithConnectionHooks registers callbacks for connection loss and for a
// completed reconnect (after subscriptions are re-established). They run on
// internal goroutines and must not block for long.
func WithConnectionHooks(onDisconnect func(error), onReconnect func()) Option {
	return func(o *options) { o.onDisconnect, o.onReconnect = onDisconnect, onReconnect }
}

// Client is a connection to a streamd server.
type Client struct {
	addr string
	opt  options

	wmu sync.Mutex // serialises writes

	// resubMu: Subscribe holds it shared while registering a subscription;
	// reconnect holds it exclusively while re-establishing them all.
	resubMu sync.RWMutex

	mu      sync.Mutex
	nc      net.Conn
	ready   chan struct{} // closed while connected; replaced on disconnect
	nextID  uint64
	pending map[uint64]chan *protocol.Frame
	subs    map[string]*Subscription
	err     error // terminal error
	done    chan struct{}
	// reconnecting is true from the moment a connection is lost until the
	// replacement is fully re-established; it keeps a single reconnect loop.
	reconnecting bool

	reconnects atomic.Int64
}

// Dial connects to addr.
func Dial(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	o := options{ping: 15 * time.Second}
	for _, f := range opts {
		f(&o)
	}
	c := &Client{addr: addr, opt: o, pending: map[uint64]chan *protocol.Frame{},
		subs: map[string]*Subscription{}, done: make(chan struct{}), ready: make(chan struct{})}
	nc, err := c.dialConn(ctx)
	if err != nil {
		return nil, err
	}
	c.nc = nc
	go c.readLoop(nc)
	if err := c.handshake(ctx, nc); err != nil {
		c.shutdown(err)
		return nil, err
	}
	close(c.ready)
	if o.ping > 0 {
		go c.pingLoop()
	}
	return c, nil
}

func (c *Client) dialConn(ctx context.Context) (net.Conn, error) {
	d := net.Dialer{Timeout: 10 * time.Second}
	nc, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
		tc.SetKeepAlive(true)
	}
	if c.opt.tls != nil {
		cfg := c.opt.tls.Clone()
		if cfg.ServerName == "" {
			host, _, _ := net.SplitHostPort(c.addr)
			cfg.ServerName = host
		}
		tc := tls.Client(nc, cfg)
		if err := tc.HandshakeContext(ctx); err != nil {
			nc.Close()
			return nil, err
		}
		nc = tc
	}
	return nc, nil
}

// handshake authenticates a fresh connection.
func (c *Client) handshake(ctx context.Context, nc net.Conn) error {
	if c.opt.token == "" {
		return nil
	}
	_, err := c.callOn(ctx, nc, &protocol.Frame{Op: protocol.OpAuth, Token: c.opt.token})
	return err
}

// Done is closed when the client is permanently finished (Close, or a lost
// connection with reconnect disabled); Err then reports why.
func (c *Client) Done() <-chan struct{} { return c.done }

// Err returns the terminal error (ErrClosed if by Close), or nil while the
// client is usable or reconnecting.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Reconnects reports how many times the client has re-established its connection.
func (c *Client) Reconnects() int64 { return c.reconnects.Load() }

// Close closes the connection and all subscriptions.
func (c *Client) Close() error {
	c.shutdown(ErrClosed)
	return nil
}

func (c *Client) shutdown(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = err
	close(c.done)
	subs := c.subs
	c.subs = map[string]*Subscription{}
	pend := c.pending
	c.pending = map[uint64]chan *protocol.Frame{}
	nc := c.nc
	c.mu.Unlock()
	if nc != nil {
		nc.Close()
	}
	for _, ch := range pend {
		close(ch)
	}
	for _, s := range subs {
		s.stop()
	}
}

func (c *Client) readLoop(nc net.Conn) {
	sc := bufio.NewScanner(nc)
	sc.Buffer(make([]byte, 0, 4096), 16<<20)
	for sc.Scan() {
		line := sc.Bytes()
		// Hot path: the server's own msg encoding, parsed without reflection.
		if mf, ok := protocol.ParseMsg(line); ok {
			c.deliver(mf)
			continue
		}
		var f protocol.Frame
		if json.Unmarshal(line, &f) != nil {
			continue
		}
		switch f.Op {
		case protocol.OpMsg:
			mf := protocol.MsgFrame{Sub: f.Sub, Topic: f.Topic, Key: f.Key, KeyEnc: f.KeyEnc,
				Value: f.Value, Enc: f.Enc, TS: f.TS}
			if f.Partition != nil {
				mf.Partition = *f.Partition
			}
			if f.Offset != nil {
				mf.Offset = *f.Offset
			}
			c.deliver(mf)
		case protocol.OpAssign:
			c.mu.Lock()
			s := c.subs[f.Sub]
			c.mu.Unlock()
			if s != nil {
				s.setAssignment(f.Parts)
			}
		case protocol.OpOK, protocol.OpErr, protocol.OpPong:
			if f.ID == 0 {
				if f.Op == protocol.OpErr {
					c.handleAsyncErr(&f)
				}
				continue
			}
			c.mu.Lock()
			ch := c.pending[f.ID]
			delete(c.pending, f.ID)
			c.mu.Unlock()
			if ch != nil {
				ch <- &f
			}
		}
	}
	err := sc.Err()
	if err == nil {
		err = errors.New("connection closed by server")
	}
	c.connLost(nc, err)
}

// deliver hands a decoded message to its subscription.
func (c *Client) deliver(mf protocol.MsgFrame) {
	c.mu.Lock()
	s := c.subs[mf.Sub]
	c.mu.Unlock()
	if s == nil {
		return
	}
	v, err := mf.ValueBytes()
	if err != nil {
		return
	}
	k, err := mf.KeyBytes()
	if err != nil {
		return
	}
	s.push(Message{Topic: mf.Topic, Partition: mf.Partition, Offset: mf.Offset,
		Timestamp: time.UnixMilli(mf.TS), Key: string(k), Value: v})
}

// connLost handles the end of a connection's read loop.
func (c *Client) connLost(nc net.Conn, cause error) {
	c.mu.Lock()
	if c.err != nil || c.nc != nc {
		c.mu.Unlock()
		return
	}
	if !c.opt.reconnect {
		c.mu.Unlock()
		c.shutdown(cause)
		return
	}
	// Go from connected to reconnecting: fail in-flight requests, drop
	// anything queued for subscriptions (it is re-fetched on resume).
	select {
	case <-c.ready:
		c.ready = make(chan struct{})
	default:
	}
	startLoop := !c.reconnecting
	c.reconnecting = true
	// From here on this connection is dead to callers: callOn refuses it, so no
	// request can register a pending reply that nothing would ever fail.
	c.nc = nil
	pend := c.pending
	c.pending = map[uint64]chan *protocol.Frame{}
	subs := make([]*Subscription, 0, len(c.subs))
	for _, s := range c.subs {
		subs = append(subs, s)
	}
	c.mu.Unlock()
	if hookAfterPendingSwap != nil {
		hookAfterPendingSwap()
	}
	nc.Close()
	for _, ch := range pend {
		close(ch)
	}
	for _, s := range subs {
		s.discardQueued()
	}
	if startLoop {
		if c.opt.onDisconnect != nil {
			c.opt.onDisconnect(cause)
		}
		go c.reconnectLoop()
	}
}

func (c *Client) reconnectLoop() {
	backoff := min(100*time.Millisecond, c.opt.maxBackoff)
	for {
		select {
		case <-c.done:
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > c.opt.maxBackoff {
			backoff = c.opt.maxBackoff
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := c.reestablish(ctx)
		cancel()
		if err == nil {
			return
		}
		if c.Err() != nil {
			return
		}
		var fatal *fatalError
		if errors.As(err, &fatal) {
			c.shutdown(fatal.err)
			return
		}
	}
}

// fatalError marks failures that retrying cannot fix (e.g. a rejected token).
type fatalError struct{ err error }

func (e *fatalError) Error() string { return e.err.Error() }

func (c *Client) reestablish(ctx context.Context) error {
	nc, err := c.dialConn(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		nc.Close()
		return ErrClosed
	}
	c.nc = nc
	c.mu.Unlock()
	go c.readLoop(nc)

	if err := c.handshake(ctx, nc); err != nil {
		nc.Close()
		if isAuthError(err) {
			return &fatalError{err}
		}
		return err
	}
	c.resubMu.Lock()
	defer c.resubMu.Unlock()
	c.mu.Lock()
	subs := make([]*Subscription, 0, len(c.subs))
	for _, s := range c.subs {
		if s.isActive() {
			subs = append(subs, s)
		}
	}
	c.mu.Unlock()
	for _, s := range subs {
		s.mu.Lock()
		s.epoch++ // acks for earlier deliveries must not reach the new server-side subscription
		s.mu.Unlock()
		r, err := c.callOn(ctx, nc, s.subscribeFrame())
		if err != nil {
			var se ServerError
			if errors.As(err, &se) {
				// The server refuses this subscription for good (topic deleted,
				// partition gone, ...). End it and keep the connection healthy.
				c.mu.Lock()
				delete(c.subs, s.id)
				c.mu.Unlock()
				s.setErr(err)
				s.stop()
				continue
			}
			nc.Close()
			return fmt.Errorf("resubscribe %s: %w", s.topic, err)
		}
		s.applySubOK(r)
		if !s.isActive() { // closed while we were resubscribing it
			c.writeOn(nc, &protocol.Frame{Op: protocol.OpUnsub, Sub: s.id})
		}
	}
	c.mu.Lock()
	if c.err != nil || c.nc != nc {
		c.mu.Unlock()
		return errors.New("connection lost during resubscribe")
	}
	close(c.ready)
	c.reconnecting = false
	c.mu.Unlock()
	c.reconnects.Add(1)
	if c.opt.onReconnect != nil {
		c.opt.onReconnect()
	}
	return nil
}

func isAuthError(err error) bool {
	return err != nil && (err.Error() == "invalid token" || err.Error() == "authentication required")
}

// handleAsyncErr routes id-less errors: those tied to a sub end that sub.
func (c *Client) handleAsyncErr(f *protocol.Frame) {
	if f.Sub != "" {
		c.mu.Lock()
		s := c.subs[f.Sub]
		delete(c.subs, f.Sub)
		c.mu.Unlock()
		if s != nil {
			s.setErr(errors.New(f.Error))
			s.stop()
			go c.write(&protocol.Frame{Op: protocol.OpUnsub, Sub: f.Sub}) // best effort
		}
	}
}

func (c *Client) pingLoop() {
	t := time.NewTicker(c.opt.ping)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			c.mu.Lock()
			nc, ready := c.nc, c.ready
			c.mu.Unlock()
			select {
			case <-ready:
			default:
				continue // reconnecting
			}
			ctx, cancel := context.WithTimeout(context.Background(), c.opt.ping)
			_, err := c.callOn(ctx, nc, &protocol.Frame{Op: protocol.OpPing})
			cancel()
			if err != nil && c.Err() == nil {
				nc.Close() // dead connection: the read loop reports it
			}
		case <-c.done:
			return
		}
	}
}

// write sends a frame on the current connection.
func (c *Client) write(f *protocol.Frame) error {
	c.mu.Lock()
	nc := c.nc
	select {
	case <-c.ready:
	default:
		err := c.err
		c.mu.Unlock()
		if err != nil {
			return err
		}
		return ErrDisconnected
	}
	c.mu.Unlock()
	return c.writeOn(nc, f)
}

func (c *Client) writeOn(nc net.Conn, f *protocol.Frame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	c.wmu.Lock()
	defer c.wmu.Unlock()
	nc.SetWriteDeadline(time.Now().Add(15 * time.Second))
	if _, err = nc.Write(b); err != nil {
		nc.Close() // the read loop reports the loss
	}
	return err
}

// call sends a request, waiting for the connection to be ready first.
func (c *Client) call(ctx context.Context, f *protocol.Frame) (*protocol.Frame, error) {
	for {
		c.mu.Lock()
		ready, nc, err := c.ready, c.nc, c.err
		c.mu.Unlock()
		if err != nil {
			return nil, err
		}
		select {
		case <-ready:
			if hookBeforeCallOn != nil {
				hookBeforeCallOn()
			}
			r, err := c.callOn(ctx, nc, f)
			if errors.Is(err, ErrDisconnected) && c.opt.reconnect && ctx.Err() == nil {
				// Raced a drop before the request was sent or answered; the
				// caller decides whether a retry is safe.
				return nil, err
			}
			return r, err
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.done:
			return nil, c.Err()
		}
	}
}

// callIdempotent is call for requests that are safe to repeat: if the
// connection drops mid-request it waits for the reconnect and sends it again
// (until ctx ends) instead of surfacing ErrDisconnected.
func (c *Client) callIdempotent(ctx context.Context, f *protocol.Frame) (*protocol.Frame, error) {
	for {
		r, err := c.call(ctx, f)
		if errors.Is(err, ErrDisconnected) && c.opt.reconnect && ctx.Err() == nil {
			continue
		}
		return r, err
	}
}

// callOn sends a request on a specific connection and waits for its reply.
func (c *Client) callOn(ctx context.Context, nc net.Conn, f *protocol.Frame) (*protocol.Frame, error) {
	ch := make(chan *protocol.Frame, 1)
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
	if c.nc != nc { // lost (or replaced) since the caller picked it
		c.mu.Unlock()
		return nil, ErrDisconnected
	}
	c.nextID++
	f.ID = c.nextID
	c.pending[f.ID] = ch
	c.mu.Unlock()
	if err := c.writeOn(nc, f); err != nil {
		c.mu.Lock()
		delete(c.pending, f.ID)
		c.mu.Unlock()
		if c.opt.reconnect {
			return nil, ErrDisconnected
		}
		return nil, err
	}
	select {
	case r, ok := <-ch:
		if !ok {
			if err := c.Err(); err != nil {
				return nil, err
			}
			return nil, ErrDisconnected
		}
		if r.Op == protocol.OpErr {
			return nil, ServerError(r.Error)
		}
		return r, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, f.ID)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Receipt identifies a published message.
type Receipt struct {
	Partition int
	Offset    int64
}

// Publish appends a message. Keyed messages hash to a stable partition;
// unkeyed ones are spread round-robin.
func (c *Client) Publish(ctx context.Context, topic, key string, value []byte) (Receipt, error) {
	return c.publish(ctx, topic, nil, key, value)
}

// PublishTo appends a message to an explicit partition.
func (c *Client) PublishTo(ctx context.Context, topic string, partition int, key string, value []byte) (Receipt, error) {
	return c.publish(ctx, topic, &partition, key, value)
}

func (c *Client) publish(ctx context.Context, topic string, partition *int, key string, value []byte) (Receipt, error) {
	f := &protocol.Frame{Op: protocol.OpPub, Topic: topic, Partition: partition}
	f.SetKey([]byte(key))
	f.SetValue(value)
	r, err := c.call(ctx, f)
	if err != nil {
		return Receipt{}, err
	}
	if r.Offset == nil || r.Partition == nil {
		return Receipt{}, errors.New("server returned no offset")
	}
	return Receipt{Partition: *r.Partition, Offset: *r.Offset}, nil
}

// Record is one message of a batch.
type Record struct {
	Key   string
	Value []byte
}

// PublishBatch appends records in one request and returns a receipt per
// record, in order. Records are routed like Publish. Records bound for the
// same partition are written atomically with one fsync, which makes this much
// faster than many Publish calls; the batch is not atomic across partitions.
func (c *Client) PublishBatch(ctx context.Context, topic string, recs []Record) ([]Receipt, error) {
	return c.publishBatch(ctx, topic, nil, recs)
}

// PublishBatchTo is PublishBatch with every record sent to one partition,
// which makes the whole batch atomic.
func (c *Client) PublishBatchTo(ctx context.Context, topic string, partition int, recs []Record) ([]Receipt, error) {
	return c.publishBatch(ctx, topic, &partition, recs)
}

func (c *Client) publishBatch(ctx context.Context, topic string, partition *int, recs []Record) ([]Receipt, error) {
	f := &protocol.Frame{Op: protocol.OpPubBatch, Topic: topic, Partition: partition,
		Msgs: make([]protocol.Msg, len(recs))}
	for i, r := range recs {
		f.Msgs[i].SetKey([]byte(r.Key))
		f.Msgs[i].SetValue(r.Value)
	}
	r, err := c.call(ctx, f)
	if err != nil {
		return nil, err
	}
	if len(r.Receipts) != len(recs) {
		return nil, fmt.Errorf("server returned %d receipts for %d records", len(r.Receipts), len(recs))
	}
	out := make([]Receipt, len(recs))
	for i, rc := range r.Receipts {
		out[i] = Receipt{Partition: rc.Partition, Offset: rc.Offset}
	}
	return out, nil
}

// DeleteTopic permanently removes a topic, its data and all consumer-group
// offsets for it. Active subscriptions to it end with an error.
func (c *Client) DeleteTopic(ctx context.Context, topic string) error {
	_, err := c.call(ctx, &protocol.Frame{Op: protocol.OpDelete, Topic: topic})
	return err
}

// ResetOffsets moves a consumer group's committed positions on a topic. to is
// Earliest, Latest or At(n) (At needs a partition). The group must have no live
// members. It returns the new next-offset per partition.
func (c *Client) ResetOffsets(ctx context.Context, topic, group, to string, partition *int) (map[int]int64, error) {
	r, err := c.call(ctx, &protocol.Frame{Op: protocol.OpReset, Topic: topic, Group: group, To: to, Partition: partition})
	if err != nil {
		return nil, err
	}
	return r.Resume, nil
}

// GroupInfo is one partition's state for a consumer group.
type GroupInfo = protocol.GroupInfo

// Groups reports committed offsets, lag and live member counts. topic and
// group filter the result when non-empty.
func (c *Client) Groups(ctx context.Context, topic, group string) ([]GroupInfo, error) {
	r, err := c.callIdempotent(ctx, &protocol.Frame{Op: protocol.OpGroups, Topic: topic, Group: group})
	if err != nil {
		return nil, err
	}
	return r.Groups, nil
}

// CreateTopic creates a topic with the given number of partitions. It is a
// no-op if the topic exists with the same count.
func (c *Client) CreateTopic(ctx context.Context, topic string, partitions int) error {
	_, err := c.callIdempotent(ctx, &protocol.Frame{Op: protocol.OpCreate, Topic: topic, Partitions: partitions})
	return err
}

// Topics lists topics and their retained offset ranges.
func (c *Client) Topics(ctx context.Context) ([]TopicInfo, error) {
	r, err := c.callIdempotent(ctx, &protocol.Frame{Op: protocol.OpTopics})
	if err != nil {
		return nil, err
	}
	return r.Topics, nil
}

// Stats returns server counters.
func (c *Client) Stats(ctx context.Context) (map[string]int64, error) {
	r, err := c.callIdempotent(ctx, &protocol.Frame{Op: protocol.OpStats})
	if err != nil {
		return nil, err
	}
	return r.Stats, nil
}

// Ping round-trips to the server.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.callIdempotent(ctx, &protocol.Frame{Op: protocol.OpPing})
	return err
}

// Start positions.
const (
	Earliest  = "earliest"
	Latest    = "latest"
	Committed = "committed"
)

// At returns a start position for a specific offset (requires a single
// partition: set SubscribeOptions.Partition for topics with more than one).
func At(offset int64) string { return strconv.FormatInt(offset, 10) }

// Partition returns a pointer for SubscribeOptions.Partition.
func Partition(n int) *int { return &n }

// SubscribeOptions configures Subscribe.
type SubscribeOptions struct {
	// Group enables committed offsets via Ack. Members of one group split a
	// topic's partitions between them and rebalance as members come and go.
	Group string
	// From is Earliest, Latest, Committed or At(n). Default: Committed with a
	// group, else Latest. With a group, Earliest/Latest only apply to
	// partitions that have no committed offset yet.
	From string
	// Partition restricts an ungrouped subscription to one partition
	// (default: all partitions, merged; order holds only within a partition).
	Partition *int
	// Window is the max unacknowledged messages in flight (0 = unlimited).
	// Set it to bound client memory: with 0 a consumer slower than the stream
	// accumulates messages in an internal queue until MaxQueued is hit.
	Window int
	Buffer int // capacity of Subscription.C (default 64)
	// MaxQueued ends the subscription with ErrSlowConsumer if more than this
	// many messages are waiting for the consumer (default 100000).
	MaxQueued int
}

// Subscription delivers messages on C, which is closed when the subscription
// or connection ends.
type Subscription struct {
	C <-chan Message

	id    string
	topic string
	opts  SubscribeOptions
	c     *Client
	out   chan Message

	mu        sync.Mutex
	cond      *sync.Cond
	queue     []Message
	closed    bool
	quit      chan struct{} // closed by stop so run never blocks on an abandoned C
	active    bool
	err       error
	epoch     uint64 // bumped each (re)subscribe; stamps delivered messages
	maxQueued int
	assigned  []int
	resume    map[int]int64 // partition -> next offset to request on resume
}

// hookBeforeCallOn lets tests pause a request after it has seen a ready
// connection but before it registers its pending reply.
var hookBeforeCallOn func()

// hookAfterPendingSwap lets tests run code in connLost's window between
// retiring the old connection's pending requests and closing its socket.
var hookAfterPendingSwap func()

// hookBeforeSubscribeCall lets tests drop the connection at the exact point
// between registering a subscription and sending its request.
var hookBeforeSubscribeCall func()

// ErrSlowConsumer ends a subscription whose consumer fell more than
// SubscribeOptions.MaxQueued messages behind.
var ErrSlowConsumer = errors.New("subscription consumer too slow: queue limit exceeded")

// Subscribe starts a subscription. It returns once the server has accepted it.
// If the connection drops while subscribing and reconnect is enabled, it waits
// for the reconnect and tries again.
func (c *Client) Subscribe(ctx context.Context, topic string, o SubscribeOptions) (*Subscription, error) {
	for {
		s, err := c.subscribeOnce(ctx, topic, o)
		if errors.Is(err, ErrDisconnected) && c.opt.reconnect && ctx.Err() == nil {
			continue
		}
		return s, err
	}
}

func (c *Client) subscribeOnce(ctx context.Context, topic string, o SubscribeOptions) (*Subscription, error) {
	// Wait out a reconnect first, so registration never races resubscription.
	c.mu.Lock()
	ready, err := c.ready, c.err
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	select {
	case <-ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.Err()
	}
	c.resubMu.RLock()
	defer c.resubMu.RUnlock()

	buf := o.Buffer
	if buf <= 0 {
		buf = 64
	}
	maxq := o.MaxQueued
	if maxq <= 0 {
		maxq = 100_000
	}
	out := make(chan Message, buf)
	s := &Subscription{C: out, out: out, topic: topic, c: c, opts: o, resume: map[int]int64{},
		quit: make(chan struct{}), maxQueued: maxq, epoch: 1}
	s.cond = sync.NewCond(&s.mu)

	// Pre-register under a client-chosen id so no message can race the reply.
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
	select {
	case <-c.ready: // still connected on the connection we will use
	default:
		c.mu.Unlock()
		return nil, ErrDisconnected
	}
	nc := c.nc
	c.nextID++
	s.id = "c" + strconv.FormatUint(c.nextID, 10)
	c.subs[s.id] = s
	c.mu.Unlock()

	if hookBeforeSubscribeCall != nil {
		hookBeforeSubscribeCall()
	}
	// callOn, not call: waiting for readiness here would deadlock against
	// reestablish, which needs resubMu exclusively before it can mark ready.
	r, err := c.callOn(ctx, nc, s.subscribeFrame())
	if err != nil {
		c.mu.Lock()
		delete(c.subs, s.id)
		c.mu.Unlock()
		if ctx.Err() != nil {
			// We gave up waiting, but the server may have accepted it: undo.
			c.write(&protocol.Frame{Op: protocol.OpUnsub, Sub: s.id})
		}
		return nil, err
	}
	s.applySubOK(r)
	s.mu.Lock()
	s.active = true
	s.mu.Unlock()
	go s.run()
	return s, nil
}

// subscribeFrame builds the sub request, including resume positions.
func (s *Subscription) subscribeFrame() *protocol.Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := &protocol.Frame{Op: protocol.OpSub, Topic: s.topic, Sub: s.id, Group: s.opts.Group,
		From: s.opts.From, Window: s.opts.Window, Partition: s.opts.Partition}
	if s.opts.Group == "" && len(s.resume) > 0 {
		f.Resume = make(map[int]int64, len(s.resume))
		for p, o := range s.resume {
			f.Resume[p] = o
		}
	}
	return f
}

// applySubOK records the server's reply to a sub request: the initial
// partition set and, for ungrouped subscriptions, where each partition starts,
// so a later reconnect can resume partitions that have not delivered yet.
func (s *Subscription) applySubOK(r *protocol.Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(r.Parts) > 0 {
		s.assigned = append([]int(nil), r.Parts...)
	}
	if s.opts.Group == "" {
		for p, o := range r.Resume {
			if _, ok := s.resume[p]; !ok {
				s.resume[p] = o
			}
		}
	}
}

func (s *Subscription) isActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active && !s.closed
}

// discardQueued drops messages not yet handed to the consumer; they are
// re-fetched from the resume position after a reconnect.
func (s *Subscription) discardQueued() {
	s.mu.Lock()
	s.queue = nil
	s.mu.Unlock()
}

// Assignment returns the partitions this subscription currently consumes. For
// group subscriptions it changes as the group rebalances.
func (s *Subscription) Assignment() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.assigned...)
}

func (s *Subscription) setAssignment(parts []int) {
	s.mu.Lock()
	s.assigned = append([]int(nil), parts...)
	s.mu.Unlock()
}

func (s *Subscription) push(m Message) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if len(s.queue) >= s.maxQueued {
		s.err = ErrSlowConsumer
		s.mu.Unlock()
		go s.Close()
		return
	}
	m.epoch = s.epoch
	s.queue = append(s.queue, m)
	s.cond.Signal()
	s.mu.Unlock()
}

func (s *Subscription) run() {
	defer close(s.out)
	for {
		s.mu.Lock()
		for len(s.queue) == 0 && !s.closed {
			s.cond.Wait()
		}
		if len(s.queue) == 0 { // closed and drained
			s.mu.Unlock()
			return
		}
		m := s.queue[0]
		s.queue[0] = Message{}
		s.queue = s.queue[1:]
		closed := s.closed
		// Record the position at hand-off, not delivery, so a resume never
		// repeats a message that is already on its way to the consumer.
		s.resume[m.Partition] = m.Offset + 1
		s.mu.Unlock()
		if closed {
			return
		}
		select {
		case s.out <- m:
		case <-s.quit:
			return
		}
	}
}

func (s *Subscription) stop() {
	s.mu.Lock()
	if !s.closed {
		close(s.quit)
	}
	s.closed = true
	s.queue = nil
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *Subscription) setErr(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

// Err reports why the subscription ended abnormally, if it did.
func (s *Subscription) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	return s.c.Err()
}

// Ack cumulatively acknowledges every message of m's partition up to and
// including m. For a group subscription this commits the group's offset; with
// a Window it also frees flow-control credit. It does not wait for the server.
// While reconnecting it returns ErrDisconnected, and for a message delivered
// before the latest reconnect it returns ErrStaleAck: neither is sent, so a
// stale ack can never commit past what the new connection has redelivered.
// Unacknowledged group messages are redelivered after the resubscribe.
func (s *Subscription) Ack(m Message) error {
	s.mu.Lock()
	cur := s.epoch
	s.mu.Unlock()
	if m.epoch != cur {
		return ErrStaleAck
	}
	return s.c.write(&protocol.Frame{Op: protocol.OpAck, Sub: s.id,
		Partition: protocol.Int(m.Partition), Offset: protocol.Int64(m.Offset)})
}

// Close ends the subscription (the connection stays open).
func (s *Subscription) Close() error {
	s.c.mu.Lock()
	delete(s.c.subs, s.id)
	s.c.mu.Unlock()
	s.stop()
	if s.c.Err() != nil {
		return nil
	}
	if err := s.c.write(&protocol.Frame{Op: protocol.OpUnsub, Sub: s.id}); errors.Is(err, ErrDisconnected) {
		return nil // already gone with the old connection
	} else {
		return err
	}
}
