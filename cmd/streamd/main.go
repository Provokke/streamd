// Command streamd runs the broker server.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Provokke/streamd/broker"
	"github.com/Provokke/streamd/commitlog"
	"github.com/Provokke/streamd/replica"
	"github.com/Provokke/streamd/server"
)

func main() {
	var (
		addr        = flag.String("addr", ":7070", "listen address")
		dir         = flag.String("data", "./data", "data directory")
		segBytes    = flag.Int64("segment-bytes", 64<<20, "roll log segments at this size")
		retAge      = flag.Duration("retention-age", 7*24*time.Hour, "delete segments older than this (0 = keep forever)")
		retBytes    = flag.Int64("retention-bytes", 0, "delete oldest segments beyond this total size per topic (0 = unlimited)")
		syncEvery   = flag.Duration("fsync-interval", 0, "0 = fsync every publish; >0 = group fsync on this interval; <0 = never")
		retEvery    = flag.Duration("retention-interval", 30*time.Second, "how often retention runs")
		token       = flag.String("token", os.Getenv("STREAMD_TOKEN"), "shared auth token (default $STREAMD_TOKEN)")
		maxConns    = flag.Int("max-conns", 10000, "max concurrent client connections")
		idle        = flag.Duration("idle-timeout", 60*time.Second, "disconnect clients silent for this long (<0 disables)")
		writeTO     = flag.Duration("write-timeout", 10*time.Second, "disconnect clients that cannot drain output within this")
		follow      = flag.String("follow", "", "leader address to replicate from (runs read-only)")
		partitions  = flag.Int("default-partitions", 1, "partitions for auto-created topics")
		noCreate    = flag.Bool("no-auto-create", false, "disallow implicit topic creation")
		httpAddr    = flag.String("http", "", "serve /healthz and /metrics on this address (e.g. 127.0.0.1:9090); unauthenticated")
		tlsCert     = flag.String("tls-cert", "", "TLS certificate file (enables TLS)")
		tlsKey      = flag.String("tls-key", "", "TLS private key file")
		tlsClient   = flag.String("tls-client-ca", "", "CA file; if set, clients must present a certificate signed by it (mutual TLS)")
		followTLS   = flag.Bool("follow-tls", false, "connect to the leader over TLS")
		followPrune = flag.Bool("follow-prune", false, "also delete local topics the leader does not have")
		followCA    = flag.String("follow-ca", "", "CA file to trust for the leader (default: system roots)")
		debug       = flag.Bool("debug", false, "debug logging")
	)
	flag.Parse()

	lvl := slog.LevelInfo
	if *debug {
		lvl = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))

	b, err := broker.Open(broker.Config{
		Dir: *dir,
		Log: commitlog.Options{SegmentBytes: *segBytes, SyncInterval: *syncEvery,
			RetentionAge: *retAge, RetentionBytes: *retBytes},
		RetentionInterval: *retEvery,
		DefaultPartitions: *partitions,
		DisableAutoCreate: *noCreate || *follow != "", // followers only get topics from the leader
		Logger:            logger,
	})
	if err != nil {
		logger.Error("open broker", "err", err)
		os.Exit(1)
	}

	var serverTLS *tls.Config
	if *tlsCert != "" || *tlsKey != "" {
		cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			logger.Error("load tls cert/key", "err", err)
			os.Exit(1)
		}
		serverTLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		if *tlsClient != "" {
			pool, err := loadPool(*tlsClient)
			if err != nil {
				logger.Error("load client CA", "err", err)
				os.Exit(1)
			}
			serverTLS.ClientCAs = pool
			serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}
	if *token != "" && serverTLS == nil {
		logger.Warn("auth token is set but TLS is not: the token travels in cleartext")
	}

	srv := server.New(b, server.Config{MaxConns: *maxConns, IdleTimeout: *idle, WriteTimeout: *writeTO,
		AuthToken: *token, ReadOnly: *follow != "", TLS: serverTLS, Logger: logger})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *follow != "" {
		f := &replica.Follower{B: b, Leader: *follow, Token: *token, Logger: logger, Prune: *followPrune}
		if *followTLS || *followCA != "" {
			f.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
			if *followCA != "" {
				pool, err := loadPool(*followCA)
				if err != nil {
					logger.Error("load follow CA", "err", err)
					os.Exit(1)
				}
				f.TLS.RootCAs = pool
			}
		}
		go f.Run(ctx)
	}

	var admin *http.Server
	if *httpAddr != "" {
		admin = &http.Server{Addr: *httpAddr, Handler: srv.AdminHandler(), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("admin http", "err", err)
			}
		}()
	}

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe(*addr) }()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-errc:
		logger.Error("server stopped", "err", err)
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if admin != nil {
		admin.Shutdown(sctx)
	}
	srv.Shutdown(sctx)
	if err := b.Close(); err != nil {
		logger.Error("close broker", "err", err)
		os.Exit(1)
	}
}

func loadPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("no certificates found in " + path)
	}
	return pool, nil
}
