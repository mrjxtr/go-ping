package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mrjxtr/go-ping/internal/config"
	"github.com/mrjxtr/go-ping/internal/handlers"
	"github.com/mrjxtr/go-ping/internal/probe"
)

func main() {
	slog.SetDefault(slog.New(&handlers.PingHandler{Out: os.Stderr}))
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

// run is the real entrypoint; main only handles the error exit so defers fire.
func run() error {
	cfg := config.NewConfig()

	var (
		timeout  = flag.Duration("timeout", 5*time.Second, "per-probe request timeout")
		interval = flag.Duration("interval", 1*time.Second, "delay between probe ticks")
		urlFlag  = flag.String(
			"url",
			"",
			"single probe url, overrides the default fallback list",
		)
	)
	flag.Parse()

	if *interval <= 0 {
		return fmt.Errorf("interval must be positive, got %s", *interval)
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be positive, got %s", *timeout)
	}

	probes := cfg.DefaultProbes
	if *urlFlag != "" {
		u, err := url.Parse(*urlFlag)
		if err != nil {
			return fmt.Errorf("invalid -url %q: %w", *urlFlag, err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf(
				"invalid -url %q: scheme must be http or https, got %q",
				*urlFlag, u.Scheme,
			)
		}
		if u.Host == "" {
			return fmt.Errorf("invalid -url %q: missing host", *urlFlag)
		}
		probes = []string{*urlFlag}
	}

	client := &http.Client{Timeout: *timeout}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// optimistic initial state: assume online. if the first tick is actually offline,
	// the online -> offline branch fires and downSince gets set correctly.
	wasOnline := true
	var downSince time.Time

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	var seq uint64
	for {
		seq++
		isOnline, res, err := probe.Probe(ctx, client, cfg.UserAgent, probes)

		if isOnline && res.Cold() {
			slog.Info("setup", probe.SetupAttrs(res)...)
		}
		switch {
		case isOnline && !wasOnline:
			slog.Info("online", probe.ProbeAttrs(seq, res, time.Since(downSince))...)
		case isOnline:
			slog.Info("online", probe.ProbeAttrs(seq, res, 0)...)
		case !isOnline:
			slog.Warn("offline", "seq", seq, "reason", probe.Classify(err))
			if wasOnline {
				downSince = time.Now()
			}
		}
		wasOnline = isOnline

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
