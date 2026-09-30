// Command streamctl is a small CLI client for streamd.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/Provokke/streamd/client"
)

const usage = `usage: streamctl [-addr host:port] [-token T] [-tls -ca ca.pem] <command> [args]

commands:
  create <topic> [-partitions N]   create a topic with N partitions (default 1)
  pub <topic> [-key K] [-partition P] [-batch N] [message]
                                   publish message (or one per stdin line if omitted;
                                   -batch N sends N lines per request)
  sub <topic> [-group G] [-partition P] [-from earliest|latest|committed|N] [-n COUNT]
                                   stream messages (acks automatically)
  delete <topic>                   delete a topic and its data (irreversible)
  reset <topic> -group G [-to earliest|latest|N] [-partition P]
                                   move a group's offsets (group must be stopped)
  groups [-topic T] [-group G]     consumer groups: committed offset, lag, members
  topics                           list topics and per-partition offset ranges
  stats                            server counters
  ping                             check connectivity
`

func main() {
	addr := flag.String("addr", "localhost:7070", "server address")
	token := flag.String("token", os.Getenv("STREAMD_TOKEN"), "auth token")
	useTLS := flag.Bool("tls", false, "connect over TLS")
	caFile := flag.String("ca", "", "CA file to trust (implies -tls; default: system roots)")
	certFile := flag.String("cert", "", "client certificate for mutual TLS")
	keyFile := flag.String("key", "", "client key for mutual TLS")
	serverName := flag.String("server-name", "", "TLS server name to verify (default: host of -addr)")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	opts := []client.Option{client.WithToken(*token)}
	if args[0] == "sub" {
		opts = append(opts, client.WithReconnect(5*time.Second))
	}
	if *useTLS || *caFile != "" || *certFile != "" {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: *serverName}
		if *caFile != "" {
			pem, err := os.ReadFile(*caFile)
			if err != nil {
				die(err)
			}
			cfg.RootCAs = x509.NewCertPool()
			if !cfg.RootCAs.AppendCertsFromPEM(pem) {
				die(fmt.Errorf("no certificates in %s", *caFile))
			}
		}
		if *certFile != "" {
			cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
			if err != nil {
				die(err)
			}
			cfg.Certificates = []tls.Certificate{cert}
		}
		opts = append(opts, client.WithTLS(cfg))
	}
	c, err := client.Dial(dctx, *addr, opts...)
	cancel()
	if err != nil {
		die(err)
	}
	defer c.Close()

	switch args[0] {
	case "create":
		err = create(ctx, c, args[1:])
	case "delete":
		if len(args) != 2 {
			die(fmt.Errorf("usage: delete <topic>"))
		}
		err = c.DeleteTopic(ctx, args[1])
	case "reset":
		err = reset(ctx, c, args[1:])
	case "groups":
		err = groups(ctx, c, args[1:])
	case "pub":
		err = pub(ctx, c, args[1:])
	case "sub":
		err = sub(ctx, c, args[1:])
	case "topics":
		var ts []client.TopicInfo
		if ts, err = c.Topics(ctx); err == nil {
			fmt.Printf("%-30s %5s %12s %12s %12s\n", "TOPIC", "PART", "EARLIEST", "NEXT", "BYTES")
			for _, t := range ts {
				for _, p := range t.Partitions {
					fmt.Printf("%-30s %5d %12d %12d %12d\n", t.Name, p.Partition, p.Earliest, p.Next, p.Bytes)
				}
			}
		}
	case "stats":
		var m map[string]int64
		if m, err = c.Stats(ctx); err == nil {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Printf("%-22s %d\n", k, m[k])
			}
		}
	case "ping":
		t0 := time.Now()
		if err = c.Ping(ctx); err == nil {
			fmt.Println("pong", time.Since(t0))
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		die(err)
	}
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func reset(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("reset", flag.ExitOnError)
	group := fs.String("group", "", "consumer group (required)")
	to := fs.String("to", "earliest", "earliest | latest | <offset>")
	part := fs.Int("partition", -1, "only this partition")
	if len(args) == 0 {
		return fmt.Errorf("reset: topic required")
	}
	fs.Parse(args[1:])
	var p *int
	if *part >= 0 {
		p = client.Partition(*part)
	}
	offs, err := c.ResetOffsets(ctx, args[0], *group, *to, p)
	if err != nil {
		return err
	}
	keys := make([]int, 0, len(offs))
	for k := range offs {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		fmt.Printf("partition %d -> next offset %d\n", k, offs[k])
	}
	return nil
}

func groups(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("groups", flag.ExitOnError)
	topic := fs.String("topic", "", "filter by topic")
	group := fs.String("group", "", "filter by group")
	fs.Parse(args)
	gs, err := c.Groups(ctx, *topic, *group)
	if err != nil {
		return err
	}
	fmt.Printf("%-20s %-24s %5s %12s %10s %8s\n", "GROUP", "TOPIC", "PART", "COMMITTED", "LAG", "MEMBERS")
	for _, g := range gs {
		fmt.Printf("%-20s %-24s %5d %12d %10d %8d\n", g.Group, g.Topic, g.Partition, g.Committed, g.Lag, g.Members)
	}
	return nil
}

func create(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	n := fs.Int("partitions", 1, "number of partitions")
	if len(args) == 0 {
		return fmt.Errorf("create: topic required")
	}
	fs.Parse(args[1:])
	return c.CreateTopic(ctx, args[0], *n)
}

func pub(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("pub", flag.ExitOnError)
	key := fs.String("key", "", "message key")
	part := fs.Int("partition", -1, "explicit partition (default: by key)")
	batch := fs.Int("batch", 1, "lines per request when reading stdin")
	// Allow the topic before flags.
	if len(args) == 0 {
		return fmt.Errorf("pub: topic required")
	}
	topic := args[0]
	fs.Parse(args[1:])
	publish := func(v []byte) error {
		var r client.Receipt
		var err error
		if *part >= 0 {
			r, err = c.PublishTo(ctx, topic, *part, *key, v)
		} else {
			r, err = c.Publish(ctx, topic, *key, v)
		}
		if err == nil {
			fmt.Printf("%d/%d\n", r.Partition, r.Offset)
		}
		return err
	}
	if fs.NArg() > 0 {
		return publish([]byte(strings.Join(fs.Args(), " ")))
	}
	flush := func(lines [][]byte) error {
		recs := make([]client.Record, len(lines))
		for i, l := range lines {
			recs[i] = client.Record{Key: *key, Value: l}
		}
		var rs []client.Receipt
		var err error
		if *part >= 0 {
			rs, err = c.PublishBatchTo(ctx, topic, *part, recs)
		} else {
			rs, err = c.PublishBatch(ctx, topic, recs)
		}
		for _, r := range rs {
			fmt.Printf("%d/%d\n", r.Partition, r.Offset)
		}
		return err
	}
	r := bufio.NewReader(os.Stdin)
	var pending [][]byte
	for {
		line, err := r.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		if line != "" {
			if *batch <= 1 {
				if perr := publish([]byte(line)); perr != nil {
					return perr
				}
			} else if pending = append(pending, []byte(line)); len(pending) >= *batch {
				if perr := flush(pending); perr != nil {
					return perr
				}
				pending = pending[:0]
			}
		}
		if err == io.EOF {
			if len(pending) > 0 {
				return flush(pending)
			}
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func sub(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("sub", flag.ExitOnError)
	group := fs.String("group", "", "consumer group")
	from := fs.String("from", "", "earliest | latest | committed | <offset>")
	part := fs.Int("partition", -1, "only this partition (ungrouped)")
	n := fs.Int("n", 0, "exit after N messages (0 = forever)")
	if len(args) == 0 {
		return fmt.Errorf("sub: topic required")
	}
	topic := args[0]
	fs.Parse(args[1:])
	o := client.SubscribeOptions{Group: *group, From: *from, Window: 1000}
	if *part >= 0 {
		o.Partition = client.Partition(*part)
	}
	s, err := c.Subscribe(ctx, topic, o)
	if err != nil {
		return err
	}
	count := 0
	for {
		select {
		case m, ok := <-s.C:
			if !ok {
				return s.Err()
			}
			fmt.Printf("%d/%d\t%s\t%s\t%s\n", m.Partition, m.Offset, m.Timestamp.Format(time.RFC3339), m.Key, m.Value)
			if err := s.Ack(m); err != nil && !errors.Is(err, client.ErrStaleAck) && !errors.Is(err, client.ErrDisconnected) {
				return err
			}
			if count++; *n > 0 && count >= *n {
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}
