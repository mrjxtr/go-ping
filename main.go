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

	"github.com/mrjxtr/go-ping/internal/handlers"
	"github.com/mrjxtr/go-ping/internal/probe"
)

const userAgent = "go-ping/1.0"

// canonical 204-no-content connectivity endpoints. tried in order, first 204 wins.
// anything else (200 with HTML, redirect, timeout) means captive portal or real outage.
var defaultProbes = []string{
	"https://www.google.com/generate_204",
	"https://connectivitycheck.gstatic.com/generate_204",
	"https://www.gstatic.com/generate_204",
}

func main() {
	slog.SetDefault(slog.New(&handlers.PingHandler{Out: os.Stderr}))
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

// run is the real entrypoint; main only handles the error exit so defers fire.
func run() error {
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

	probes := defaultProbes
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
		isOnline, res, err := probe.Probe(ctx, client, probes)

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
