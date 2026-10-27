# streamd

A lightweight, persistent pub/sub and event-streaming broker in Go (standard
library only). Topics are split into partitions, each backed by a segmented,
checksummed commit log. Consumers replay from any retained offset, consumer
groups share partitions and keep durable offsets, and the TCP layer manages
client sockets defensively.

```
 publishers ─┐                          ┌─ subscriber (replay / live tail)
 publishers ─┼─ TCP/TLS, JSON lines ─►  server ─► broker ─► partition logs ─► disk
 subscribers ┘   auth · limits ·                  │ group coordinator (rebalance)
                 idle/slow handling               │ offsets.json · retention
                                                  └ async follower replication
```

## Quick start

```sh
make build            # or: go install github.com/Provokke/streamd/cmd/...@latest
./bin/streamd -addr :7070 -data ./data -http 127.0.0.1:9090 &

./bin/streamctl create orders -partitions 3
./bin/streamctl pub orders -key user-42 '{"item":"saddle"}'   # -> 1/0  (partition/offset)
./bin/streamctl sub orders -from earliest                      # replay + live, all partitions
./bin/streamctl sub orders -group billing                      # run several: they split partitions
./bin/streamctl topics
seq 1 1000 | ./bin/streamctl pub orders -batch 100            # batched publish from stdin
./bin/streamctl groups                                         # committed offset, lag, members
./bin/streamctl reset orders -group billing -to earliest       # replay a stopped group
./bin/streamctl delete orders                                  # irreversible
curl localhost:9090/metrics
```

Go client:

```go
c, _ := client.Dial(ctx, "localhost:7070",
    client.WithToken(tok), client.WithReconnect(10*time.Second))
c.CreateTopic(ctx, "orders", 3)
rcpt, _ := c.Publish(ctx, "orders", "user-42", []byte("hello")) // rcpt.Partition, rcpt.Offset

sub, _ := c.Subscribe(ctx, "orders", client.SubscribeOptions{Group: "billing", Window: 100})
for m := range sub.C {
    handle(m)   // must be idempotent: delivery is at-least-once
    sub.Ack(m)  // commits this partition's offset, frees flow-control credit
}
```

## Features

| Area | What you get |
|---|---|
| Persistence | Append-only segments, CRC32C per record, sparse offset index; fsync per publish (default), group-fsync interval, or none |
| Crash recovery | Torn/corrupt tail of the newest segment is truncated on open; corruption in older segments fails loudly. Fuzz-tested. |
| Batching | `PublishBatch`: many records per request, atomic and one fsync per partition (~6x the throughput of single publishes at default durability) |
| Operations | Delete topics, reset a stopped group's offsets (earliest/latest/offset), inspect groups with lag and member counts |
| Partitions | Per-topic partition count; stable key hashing, round-robin for unkeyed, or explicit partition. Ordering holds per partition. |
| Consumer groups | Members split partitions and rebalance on join/leave/disconnect; durable per-partition offsets (atomic write-rename) |
| Replay | Subscribe from `earliest`, `latest`, `committed`, or an exact offset; offsets below retention clamp to earliest |
| Retention | Per-partition by age and/or total bytes; whole closed segments are deleted |
| Flow control | Per-subscription `window` of unacked messages; credits are refunded when partitions are revoked |
| Socket management | Max connections, auth token, TLS / mutual TLS, handshake deadline, idle timeout, TCP keepalive, bounded write queue, slow-consumer disconnect, oversized-frame rejection, graceful shutdown |
| Client | Multiplexed requests + subscriptions on one connection; optional auto-reconnect that re-authenticates and resumes subscriptions, retries idempotent calls transparently, and drops stale acks (`ErrStaleAck`) so a pre-reconnect ack can never commit past redelivered messages |
| Replication | `streamd -follow leader:7070` mirrors all topics and partitions (same offsets/timestamps), committed consumer-group positions, and topic deletions/recreations; read-only. Followers never auto-create topics, and a follower can be promoted by restarting it without `-follow` |
| Observability | `stats` op, plus `-http` serving `/healthz` and Prometheus `/metrics` (counters, per-partition offsets and size, consumer-group lag) |

Wire protocol: [docs/protocol.md](docs/protocol.md).

## Operating it

```sh
# TLS (+ optional mutual TLS) and auth
./bin/streamd -tls-cert server.pem -tls-key server.key [-tls-client-ca clients-ca.pem] -token "$TOKEN"
./bin/streamctl -tls -ca ca.pem -token "$TOKEN" topics

# Follower (read-only mirror), optionally over TLS
./bin/streamd -addr :7071 -data ./data2 -follow leader:7070 -follow-ca ca.pem -token "$TOKEN"
```

Notable flags: `-default-partitions`, `-segment-bytes`, `-retention-age`,
`-retention-bytes`, `-fsync-interval`, `-max-conns`, `-idle-timeout`,
`-write-timeout`, `-no-auto-create`. Run `streamd -h` for all of them.

### Promoting a follower

Replication is asynchronous and there is no election: failover is a manual,
deliberate step.

1. Stop the leader (or confirm it is gone) so two nodes never accept writes.
2. Restart the follower **without** `-follow` on the same `-data` directory. It
   serves the mirrored logs as a normal leader, and each consumer group resumes
   at the position last synced from the old leader (clamped to what the follower
   has).
3. Point clients (and any new follower) at it.

Anything the old leader acknowledged but the follower had not yet received is
lost; positions are synced every `DiscoverInterval` (2s), so a group may replay
a few recently-acked messages. By default a follower only deletes local topics
that it saw on the leader and were later deleted there; `-follow-prune` also
removes local topics the leader has never had.

## Layout

```
commitlog/  segmented log: append, read, recovery, retention
broker/     topics/partitions, group coordinator, consumer offsets, retention scheduler
protocol/   wire frames
server/     TCP/TLS server, connections, subscriptions, admin HTTP handler
client/     Go client (multiplexed, reconnecting)
replica/    async leader -> follower replication
cmd/streamd, cmd/streamctl
```

## Guarantees and limits (read before relying on it)

- **Delivery is at-least-once** for group consumers. Offsets commit on `Ack` and
  are flushed to disk within ~200 ms, so a crash, reconnect or rebalance can
  replay recently handled messages. Make handlers idempotent. A retried
  `Publish` after a lost connection can duplicate.
- **Ordering is per partition**, not per topic. Key your messages to keep related
  events together. The partition count is fixed at creation.
- **Rebalancing is stop-the-world per affected partition** (no cooperative
  handoff); a partition can be briefly consumed by neither member.
- **Replication is asynchronous with no automatic failover or election.** A
  follower can lag, and promoting it is manual (see above); this is not a
  consensus cluster, and writes acknowledged by a leader that dies before they
  replicate are lost.
- Publish is acknowledged after the write (and fsync with the default
  `-fsync-interval 0`). With a positive interval, up to that window of
  acknowledged messages can be lost on power failure.
- Auth is a single shared token; use TLS on untrusted networks (the server warns
  if a token is set without TLS). The `-http` endpoint is unauthenticated: bind it
  to a private interface.
- Retention works on whole segments, so retained data slightly exceeds the limits.
- `pubbatch` is atomic per partition, not across partitions (see the protocol doc).
  `Subscription.Close` discards messages not yet handed to the consumer.
- A consumer slower than the stream with `Window: 0` buffers up to `MaxQueued`
  (100k) messages client-side, then its subscription ends with `ErrSlowConsumer`.
  Set `Window` and `Ack` to apply backpressure instead.
- Throughput is bounded by JSON framing and one log per partition. Indicative
  numbers on a 4-core sandbox (`go test -bench`), not a benchmark report:
  ~650k appends/s unsynced and ~4.8k/s with an fsync per record; ~540k msgs/s
  delivered to one subscriber and ~1M msgs/s aggregate across eight (the
  delivery path uses a hand-written encoder and a fuzz-verified fast parser);
  ~30k msgs/s publishing in batches of 100 with an fsync per partition per batch.

## Testing

`make race` runs everything under the race detector. Beyond ordinary unit and
integration tests, the suite includes:

- a **model-based randomized test** of the commit log (random appends, batches,
  reads, reopen, torn tails, retention, replicate, reset, compared against an
  in-memory model; `MODEL_SEEDS=2000` for a long soak);
- **fuzzers** for crash recovery (`FuzzRecover`) and for the wire fast path,
  differentially checked against `encoding/json` (`FuzzParseMsgDifferential`);
- **fault injection**: failed batch writes at every position, including across
  segment rolls, must roll back completely;
- **network chaos** via a severable TCP proxy: reconnects during outages, flapping
  connections while subscribing, group churn that must never lose a message;
- protocol abuse, goroutine-leak and slow-consumer isolation tests.

The regression tests for the concurrency bugs found in review were
mutation-checked: reverting each fix makes its test fail.

## Development

```sh
make short    # quick loop (skips the e2e binaries)
make race     # full suite under the race detector, including e2e
make vet      # go vet + gofmt check
make cover    # coverage across packages
make fuzz     # 20s smoke run of each fuzzer
make soak     # long randomized run of the commit-log model test
make bench
```

## License

MIT, see [LICENSE](LICENSE).
