# Wire protocol

Newline-delimited JSON over TCP (optionally TLS): one object per line, both
directions, max line size 2 MiB by default. Easy to poke with `nc` or `openssl s_client`.
All fields are optional except `op`; see `protocol/protocol.go` for the canonical definition.

Requests carry an `id`; the server echoes it in the `ok` / `err` reply. `ack`
and `unsub` are fire-and-forget (the server replies only on error, with `id` 0).

## Client → server

| op | fields | reply |
|---|---|---|
| `auth` | `token` | `ok` / `err` (connection closed on failure). Must be first when the server has a token. |
| `ping` | | `pong` |
| `create` | `topic`, `partitions` (default 1) | `ok`. Idempotent for the same count; a different count is an error. |
| `delete` | `topic` | `ok`. Removes the topic, its data and every group's offsets. Subscribers get `err` (`topic deleted`) for their `sub`. |
| `pub` | `topic`, `key`, `kenc`, `value`, `enc`, `partition` | `ok` with `partition` and `offset`. Without `partition`, keyed messages use a stable hash, unkeyed ones round-robin. |
| `pubbatch` | `topic`, `msgs[]` (`key`,`kenc`,`value`,`enc`), `partition` | `ok` with `receipts[]` (`partition`,`offset`) in message order. Max 10000 messages. |
| `reset` | `topic`, `group`, `to` (`earliest`\|`latest`\|offset), `partition` | `ok` with `resume` = new next offset per partition. Refused while the group has live members. |
| `groups` | `topic`, `group` (both optional filters) | `ok` with `groups[]` (`group`,`topic`,`partition`,`committed`,`lag`,`members`) |
| `sub` | `topic`, `sub`, `group`, `from`, `partition`, `window`, `resume` | `ok` (with `parts` for ungrouped subs), then `msg` frames |
| `ack` | `sub`, `partition`, `offset` | none |
| `unsub` | `sub` | none |
| `topics` | | `ok` with `topics[]` (per-partition `earliest`/`next`/`bytes`) |
| `stats` | | `ok` with `stats{}` |

### `sub`

- `from`: `earliest`, `latest`, `committed`, or a decimal offset (single partition only).
  Default is `committed` with a group, `latest` without.
- `partition`: restrict an ungrouped subscription to one partition. Omitted = all partitions.
- `group`: join a consumer group. Partitions are split across members, and the server
  sends `assign` frames whenever the assignment changes. For partitions with a
  committed offset the group resumes there; `earliest`/`latest` only decide the start for
  partitions with none.
- `ok` for an ungrouped sub carries `parts` (the partitions streamed) and `resume`
  (the offset each starts from), which a client can store to resume exactly.
- `window`: maximum unacknowledged messages in flight across all of the subscription's partitions.
  `0` = unlimited. Acks return credit.
- `resume`: `{"<partition>": nextOffset}` overriding `from` for ungrouped subs (used by the client on reconnect).

A subscriber whose start offset has been deleted by retention is moved to the
earliest retained offset.

### `ack`

Cumulative per partition: acknowledges every message of that partition up to and
including `offset`. For group subscriptions it commits `offset+1` as the group's
position. Acks for partitions the member no longer owns (after a rebalance) are ignored.
When a group member first takes a partition that has no committed offset, the starting
position it resolved (`earliest` or `latest`) is committed immediately, so a reconnect
before the first ack does not re-resolve `latest` and skip messages.

## Server → client

| op | meaning |
|---|---|
| `ok` / `err` | reply to the request with the same `id` (`err` has `error`). An `err` with `sub` and no `id` ends that subscription. |
| `pong` | reply to `ping` |
| `msg` | `sub`, `topic`, `partition`, `offset`, `ts` (unix ms), `key`, `value`, `enc` |
| `assign` | `sub`, `topic`, `parts`: partitions this subscription now owns (group members, after every rebalance) |

`value` is the payload as a JSON string. Non-UTF-8 payloads are sent base64-encoded
with `"enc":"base64"`; keys likewise with `"kenc":"base64"`. Both flags are honoured on
`pub` and `pubbatch`, so arbitrary binary keys and values round-trip.

### `msg` encoding

The server emits `msg` frames with a fixed field order
(`op, sub, topic, [key, kenc, value, enc], partition, offset, ts`) from a hand-written
encoder, and the Go client reads that exact shape with a strict fast-path parser,
falling back to `encoding/json` for anything else. Other clients should just use a
normal JSON decoder; the fast path is purely an optimisation and is differentially
fuzzed against `encoding/json` (`protocol/fastmsg_test.go`).

### `pubbatch` semantics

Messages are routed like `pub` (or all to `partition` if given). All sizes are
validated before anything is written. Messages bound for the same partition are
appended atomically, in order, with one fsync. The batch is **not** atomic across
partitions: after an I/O error some partitions may have committed. Use `partition` to
make a whole batch atomic.

## Example

```
-> {"op":"create","id":1,"topic":"orders","partitions":3}
<- {"op":"ok","id":1}
-> {"op":"pub","id":2,"topic":"orders","key":"user-42","value":"hello"}
<- {"op":"ok","id":2,"partition":1,"offset":0}
-> {"op":"sub","id":3,"topic":"orders","group":"billing","from":"earliest","window":100}
<- {"op":"ok","id":3,"sub":"s1"}
<- {"op":"assign","sub":"s1","topic":"orders","parts":[0,1,2]}
<- {"op":"msg","sub":"s1","topic":"orders","key":"user-42","value":"hello","partition":1,"offset":0,"ts":1730611200000}
-> {"op":"ack","sub":"s1","partition":1,"offset":0}
```

## Group rebalancing

A group's members are ordered by join time and partition *p* goes to member
*p mod n*. Every join, leave or disconnect recomputes this and sends new
`assign` frames. Revoked partitions stop streaming immediately; their unacked
messages are redelivered to the new owner from the committed offset, so
processing is **at-least-once**. There is no cooperative handoff, so a rebalance
briefly pauses affected partitions.
