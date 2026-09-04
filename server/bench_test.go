package server_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/Provokke/streamd/broker"
	"github.com/Provokke/streamd/client"
	"github.com/Provokke/streamd/commitlog"
	"github.com/Provokke/streamd/server"
)

// BenchmarkPublishParallel measures end-to-end publish round trips over TCP
// with several client connections (group-fsync off, to measure the broker not the disk).
func BenchmarkPublishParallel(b *testing.B) {
	e := start(b, b.TempDir(), broker.Config{Log: commitlog.Options{SyncInterval: -1}, DefaultPartitions: 4}, server.Config{})
	defer e.stop()
	ctx := context.Background()
	const conns = 8
	cs := make([]*client.Client, conns)
	for i := range cs {
		c, err := client.Dial(ctx, e.addr)
		if err != nil {
			b.Fatal(err)
		}
		defer c.Close()
		cs[i] = c
	}
	val := make([]byte, 128)
	b.SetBytes(int64(len(val)))
	b.ResetTimer()
	var wg sync.WaitGroup
	per := b.N / conns
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(c *client.Client, i int) {
			defer wg.Done()
			for j := 0; j < per; j++ {
				if _, err := c.Publish(ctx, "bench", fmt.Sprint(i), val); err != nil {
					b.Error(err)
					return
				}
			}
		}(cs[i], i)
	}
	wg.Wait()
}

// BenchmarkFanout measures delivery of a published stream to many subscribers.
func BenchmarkFanout(b *testing.B) {
	e := start(b, b.TempDir(), broker.Config{Log: commitlog.Options{SyncInterval: -1}}, server.Config{})
	defer e.stop()
	ctx := context.Background()
	const subs = 10
	var ss []*client.Subscription
	for i := 0; i < subs; i++ {
		c, _ := client.Dial(ctx, e.addr)
		defer c.Close()
		s, err := c.Subscribe(ctx, "fan", client.SubscribeOptions{Buffer: 1024})
		if err != nil {
			b.Fatal(err)
		}
		ss = append(ss, s)
	}
	pc, _ := client.Dial(ctx, e.addr)
	defer pc.Close()
	val := make([]byte, 128)
	b.ResetTimer()
	go func() {
		for i := 0; i < b.N; i++ {
			pc.Publish(ctx, "fan", "", val)
		}
	}()
	var wg sync.WaitGroup
	for _, s := range ss {
		wg.Add(1)
		go func(s *client.Subscription) {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				<-s.C
			}
		}(s)
	}
	wg.Wait()
	b.ReportMetric(float64(b.N*subs)/b.Elapsed().Seconds(), "deliveries/s")
}

// BenchmarkDelivery measures raw delivery throughput: the log is preloaded,
// then n subscribers each replay it from the start. The client's decode work
// is included (it is part of any real consumer).
func BenchmarkDelivery(b *testing.B) {
	for _, subs := range []int{1, 8} {
		b.Run(fmt.Sprintf("subs=%d", subs), func(b *testing.B) {
			e := start(b, b.TempDir(), broker.Config{Log: commitlog.Options{SyncInterval: -1}}, server.Config{})
			defer e.stop()
			ctx := context.Background()
			pc, _ := client.Dial(ctx, e.addr)
			defer pc.Close()
			const msgs = 50000
			val := make([]byte, 128)
			for i := range val {
				val[i] = byte('a' + i%26)
			}
			recs := make([]client.Record, 500)
			for i := range recs {
				recs[i] = client.Record{Key: "k", Value: val}
			}
			for i := 0; i < msgs/len(recs); i++ {
				if _, err := pc.PublishBatchTo(ctx, "d", 0, recs); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				var wg sync.WaitGroup
				for i := 0; i < subs; i++ {
					c, err := client.Dial(ctx, e.addr)
					if err != nil {
						b.Fatal(err)
					}
					s, err := c.Subscribe(ctx, "d", client.SubscribeOptions{From: client.Earliest, Buffer: 4096})
					if err != nil {
						b.Fatal(err)
					}
					wg.Add(1)
					go func() {
						defer wg.Done()
						defer c.Close()
						for i := 0; i < msgs; i++ {
							<-s.C
						}
					}()
				}
				wg.Wait()
			}
			b.ReportMetric(float64(b.N*msgs*subs)/b.Elapsed().Seconds(), "msgs/s")
		})
	}
}

// BenchmarkPublishBatch measures one connection publishing 100-record batches
// (compare with BenchmarkPublishParallel, which sends records one at a time).
func BenchmarkPublishBatch(b *testing.B) {
	e := start(b, b.TempDir(), broker.Config{Log: commitlog.Options{SyncInterval: -1}, DefaultPartitions: 4}, server.Config{})
	defer e.stop()
	ctx := context.Background()
	c, err := client.Dial(ctx, e.addr)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	recs := make([]client.Record, 100)
	for i := range recs {
		recs[i] = client.Record{Key: fmt.Sprint(i), Value: make([]byte, 128)}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.PublishBatch(ctx, "pb", recs); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*len(recs))/b.Elapsed().Seconds(), "msgs/s")
}

// BenchmarkPublishBatchFsync is the same with an fsync per partition per
// batch, i.e. the default durability setting.
func BenchmarkPublishBatchFsync(b *testing.B) {
	e := start(b, b.TempDir(), broker.Config{DefaultPartitions: 4}, server.Config{})
	defer e.stop()
	ctx := context.Background()
	c, _ := client.Dial(ctx, e.addr)
	defer c.Close()
	recs := make([]client.Record, 100)
	for i := range recs {
		recs[i] = client.Record{Key: fmt.Sprint(i), Value: make([]byte, 128)}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.PublishBatch(ctx, "pb", recs); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*len(recs))/b.Elapsed().Seconds(), "msgs/s")
}
