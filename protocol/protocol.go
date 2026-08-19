// Package protocol defines the newline-delimited JSON wire format shared by
// the server and client. One JSON object per line, in both directions.
//
// Client -> server ops: auth, ping, create, delete, pub, pubbatch, sub, ack, unsub,
// reset, topics, groups, stats.
// Server -> client ops: ok, err, pong, msg, assign.
//
// Requests carry an "id" that the server echoes in "ok"/"err". "ack" and
// "unsub" are fire-and-forget: the server replies only on error.
package protocol

import (
	"encoding/base64"
	"unicode/utf8"
)

// Op names.
const (
	OpAuth     = "auth"
	OpPing     = "ping"
	OpCreate   = "create"
	OpPub      = "pub"
	OpPubBatch = "pubbatch"
	OpDelete   = "delete"
	OpReset    = "reset"
	OpGroups   = "groups"
	OpSub      = "sub"
	OpAck      = "ack"
	OpUnsub    = "unsub"
	OpTopics   = "topics"
	OpStats    = "stats"

	OpOK   = "ok"
	OpErr  = "err"
	OpPong = "pong"
	OpMsg  = "msg"
	// OpAssign tells a subscriber which partitions it now owns (sent on
	// subscribe for group members and after every group rebalance).
	OpAssign = "assign"
)

// EncBase64 marks Value as base64 (used for non-UTF-8 payloads).
const EncBase64 = "base64"

// PartitionInfo is one partition's retained offset range.
type PartitionInfo struct {
	Partition int   `json:"partition"`
	Earliest  int64 `json:"earliest"`
	Next      int64 `json:"next"`
	Bytes     int64 `json:"bytes"`
}

// TopicInfo describes a topic on the wire.
type TopicInfo struct {
	Name       string          `json:"name"`
	Partitions []PartitionInfo `json:"partitions"`
}

// Msg is one message of a pubbatch.
type Msg struct {
	Key    string `json:"key,omitempty"`
	KeyEnc string `json:"kenc,omitempty"`
	Value  string `json:"value,omitempty"`
	Enc    string `json:"enc,omitempty"`
}

// SetKey and SetValue encode like Frame's.
func (m *Msg) SetKey(k []byte) { f := Frame{}; f.SetKey(k); m.Key, m.KeyEnc = f.Key, f.KeyEnc }

// SetValue encodes the payload, base64 if not valid UTF-8.
func (m *Msg) SetValue(v []byte) { f := Frame{}; f.SetValue(v); m.Value, m.Enc = f.Value, f.Enc }

// Receipt locates a published message.
type Receipt struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
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

// Frame is the single message shape for all ops; unused fields are omitted.
type Frame struct {
	Op     string `json:"op"`
	ID     uint64 `json:"id,omitempty"`
	Topic  string `json:"topic,omitempty"`
	Key    string `json:"key,omitempty"`
	KeyEnc string `json:"kenc,omitempty"` // "base64" if Key is not valid UTF-8
	Value  string `json:"value,omitempty"`
	Enc    string `json:"enc,omitempty"`

	// Subscription fields.
	Sub    string `json:"sub,omitempty"`
	Group  string `json:"group,omitempty"`
	From   string `json:"from,omitempty"`   // earliest | latest | committed | <offset> (offset needs one partition)
	Window int    `json:"window,omitempty"` // max unacked messages in flight (0 = unlimited)

	// Partition: pub target (omit to pick by key), sub filter (omit for all
	// partitions), and the partition of a msg/ack. Offsets are per partition.
	Partition  *int  `json:"partition,omitempty"`
	Partitions int   `json:"partitions,omitempty"` // create: partition count
	Parts      []int `json:"parts,omitempty"`      // assign: owned partitions

	// Resume (sub): per-partition next offsets that override From, used by
	// clients re-establishing an ungrouped subscription after a reconnect.
	// In an ungrouped sub's ok reply it carries the offsets each partition
	// starts streaming from, so a client can resume exactly.
	Resume map[int]int64 `json:"resume,omitempty"`

	Offset *int64 `json:"offset,omitempty"`
	TS     int64  `json:"ts,omitempty"` // unix ms
	Token  string `json:"token,omitempty"`

	Msgs     []Msg       `json:"msgs,omitempty"`     // pubbatch
	Receipts []Receipt   `json:"receipts,omitempty"` // pubbatch reply
	To       string      `json:"to,omitempty"`       // reset target
	Groups   []GroupInfo `json:"groups,omitempty"`   // groups reply

	Error  string           `json:"error,omitempty"`
	Topics []TopicInfo      `json:"topics,omitempty"`
	Stats  map[string]int64 `json:"stats,omitempty"`
}

// SetValue stores b in Value, base64-encoding it if it is not valid UTF-8.
func (f *Frame) SetValue(b []byte) {
	if utf8.Valid(b) {
		f.Value, f.Enc = string(b), ""
		return
	}
	f.Value, f.Enc = base64.StdEncoding.EncodeToString(b), EncBase64
}

// SetKey stores k in Key, base64-encoding it if it is not valid UTF-8.
func (f *Frame) SetKey(k []byte) {
	if utf8.Valid(k) {
		f.Key, f.KeyEnc = string(k), ""
		return
	}
	f.Key, f.KeyEnc = base64.StdEncoding.EncodeToString(k), EncBase64
}

// KeyBytes returns the key bytes, decoding base64 if flagged.
func (f *Frame) KeyBytes() ([]byte, error) {
	if f.KeyEnc == EncBase64 {
		return base64.StdEncoding.DecodeString(f.Key)
	}
	return []byte(f.Key), nil
}

// ValueBytes returns the payload bytes, decoding base64 if flagged.
func (f *Frame) ValueBytes() ([]byte, error) {
	if f.Enc == EncBase64 {
		return base64.StdEncoding.DecodeString(f.Value)
	}
	return []byte(f.Value), nil
}

// Int returns a pointer to v (for Frame.Partition).
func Int(v int) *int { return &v }

// Int64 returns a pointer to v (for Frame.Offset).
func Int64(v int64) *int64 { return &v }
