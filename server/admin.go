package server

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// AdminHandler returns an HTTP handler serving:
//
//	GET /healthz  200 "ok" while serving, 503 after Shutdown
//	GET /metrics  Prometheus text exposition: server counters, per-partition
//	              offsets and sizes, and consumer-group lag
//
// It is unauthenticated; bind it to a private interface.
func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		shut := s.shut
		s.mu.Unlock()
		if shut {
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		s.writeMetrics(w)
	})
	return mux
}

var (
	gauges = map[string]string{"connections": "Current client connections.", "subscriptions": "Current subscriptions."}
	help   = map[string]string{
		"connections_total":    "Connections accepted since start.",
		"connections_rejected": "Connections refused (over the connection limit).",
		"slow_disconnects":     "Clients disconnected for not draining output.",
		"idle_disconnects":     "Clients disconnected for being idle.",
		"messages_in":          "Messages published.",
		"messages_out":         "Messages delivered to subscribers.",
		"bytes_in":             "Bytes received from clients.",
		"bytes_out":            "Bytes sent to clients.",
	}
)

func (s *Server) writeMetrics(w http.ResponseWriter) {
	st := s.Stats()
	keys := make([]string, 0, len(st))
	for k := range st {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if h, ok := gauges[k]; ok {
			fmt.Fprintf(w, "# HELP streamd_%s %s\n# TYPE streamd_%s gauge\nstreamd_%s %d\n", k, h, k, k, st[k])
			continue
		}
		name := "streamd_" + strings.TrimSuffix(k, "_total") + "_total"
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help[k], name, name, st[k])
	}

	topics := s.b.Topics()
	type pk struct {
		topic string
		part  int
	}
	next := map[pk]int64{}
	fmt.Fprint(w, "# HELP streamd_partition_earliest_offset Oldest retained offset.\n# TYPE streamd_partition_earliest_offset gauge\n")
	for _, t := range topics {
		for _, p := range t.Partitions {
			next[pk{t.Name, p.Partition}] = p.Next
			fmt.Fprintf(w, "streamd_partition_earliest_offset{topic=%s,partition=\"%d\"} %d\n", q(t.Name), p.Partition, p.Earliest)
		}
	}
	fmt.Fprint(w, "# HELP streamd_partition_next_offset Offset the next message will get.\n# TYPE streamd_partition_next_offset gauge\n")
	for _, t := range topics {
		for _, p := range t.Partitions {
			fmt.Fprintf(w, "streamd_partition_next_offset{topic=%s,partition=\"%d\"} %d\n", q(t.Name), p.Partition, p.Next)
		}
	}
	fmt.Fprint(w, "# HELP streamd_partition_bytes Bytes on disk.\n# TYPE streamd_partition_bytes gauge\n")
	for _, t := range topics {
		for _, p := range t.Partitions {
			fmt.Fprintf(w, "streamd_partition_bytes{topic=%s,partition=\"%d\"} %d\n", q(t.Name), p.Partition, p.Bytes)
		}
	}
	fmt.Fprint(w, "# HELP streamd_group_lag Messages a consumer group has yet to read.\n# TYPE streamd_group_lag gauge\n")
	for _, g := range s.b.Offsets().Snapshot() {
		n, ok := next[pk{g.Topic, g.Partition}]
		if !ok {
			continue
		}
		lag := max(n-g.Next, 0)
		fmt.Fprintf(w, "streamd_group_lag{group=%s,topic=%s,partition=\"%d\"} %d\n", q(g.Group), q(g.Topic), g.Partition, lag)
	}
}

// q quotes a Prometheus label value.
func q(v string) string {
	v = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v)
	return `"` + v + `"`
}
