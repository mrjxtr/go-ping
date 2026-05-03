package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const userAgent = "ping/1.0"

// canonical 204-no-content connectivity endpoints. tried in order, first 204 wins.
// anything else (200 with HTML, redirect, timeout) means captive portal or real outage.
var defaultProbes = []string{
	"https://www.google.com/generate_204",
	"https://connectivitycheck.gstatic.com/generate_204",
	"https://www.gstatic.com/generate_204",
}

func main() {
	slog.SetDefault(slog.New(&pingHandler{out: os.Stderr}))
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
		isOnline, res, err := probe(ctx, client, probes)

		if isOnline && res.cold() {
			slog.Info("setup", setupAttrs(res)...)
		}
		switch {
		case isOnline && !wasOnline:
			slog.Info("online", probeAttrs(seq, res, time.Since(downSince))...)
		case isOnline:
			slog.Info("online", probeAttrs(seq, res, 0)...)
		case !isOnline:
			slog.Warn("offline", "seq", seq, "reason", classify(err))
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

// probeResult is the timing breakdown for one successful probe.
type probeResult struct {
	target string
	rtt    time.Duration
	dns    time.Duration
	tcp    time.Duration
	tls    time.Duration
}

// cold reports whether this probe had to dial fresh (DNS/TCP/TLS were paid).
func (r probeResult) cold() bool { return r.dns > 0 || r.tcp > 0 || r.tls > 0 }

// probe tries each url in order and returns on the first 204; on full failure
// returns the last error.
func probe(
	ctx context.Context,
	client *http.Client,
	urls []string,
) (bool, probeResult, error) {
	var lastErr error
	for _, probeURL := range urls {
		if ctx.Err() != nil {
			return false, probeResult{}, ctx.Err()
		}
		res, err := check(ctx, client, probeURL)
		if err == nil {
			return true, res, nil
		}
		lastErr = err
	}
	return false, probeResult{}, lastErr
}

// check fires one request and returns the timing breakdown on a 204; any other
// status is a failure (likely captive portal). RTT is "request written → first
// response byte" via httptrace; setup phases are filled only on a fresh dial.
func check(ctx context.Context, client *http.Client, url string) (probeResult, error) {
	var (
		dnsStart, dnsDone    time.Time
		connStart, connDone  time.Time
		tlsStart, tlsDone    time.Time
		wroteAt, firstByteAt time.Time
		reused               bool
	)
	trace := &httptrace.ClientTrace{
		DNSStart:             func(httptrace.DNSStartInfo) { dnsStart = time.Now() },
		DNSDone:              func(httptrace.DNSDoneInfo) { dnsDone = time.Now() },
		ConnectStart:         func(string, string) { connStart = time.Now() },
		ConnectDone:          func(string, string, error) { connDone = time.Now() },
		TLSHandshakeStart:    func() { tlsStart = time.Now() },
		TLSHandshakeDone:     func(tls.ConnectionState, error) { tlsDone = time.Now() },
		GotConn:              func(info httptrace.GotConnInfo) { reused = info.Reused },
		WroteRequest:         func(httptrace.WroteRequestInfo) { wroteAt = time.Now() },
		GotFirstResponseByte: func() { firstByteAt = time.Now() },
	}

	req, err := http.NewRequestWithContext(
		httptrace.WithClientTrace(ctx, trace),
		http.MethodGet, url, nil,
	)
	if err != nil {
		return probeResult{}, err
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return probeResult{}, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusNoContent {
		return probeResult{}, fmt.Errorf(
			"unexpected status %d (possible captive portal)",
			resp.StatusCode,
		)
	}

	res := probeResult{target: url, rtt: firstByteAt.Sub(wroteAt)}
	if !reused {
		// guard each phase: a cached DNS lookup may not fire DNSStart, and an
		// http (not https) request never fires the TLS hooks at all.
		if !dnsStart.IsZero() && !dnsDone.IsZero() {
			res.dns = dnsDone.Sub(dnsStart)
		}
		if !connStart.IsZero() && !connDone.IsZero() {
			res.tcp = connDone.Sub(connStart)
		}
		if !tlsStart.IsZero() && !tlsDone.IsZero() {
			res.tls = tlsDone.Sub(tlsStart)
		}
	}
	return res, nil
}

// probeAttrs builds the slog attr list for an "online" log line.
func probeAttrs(seq uint64, res probeResult, downtime time.Duration) []any {
	attrs := []any{
		"seq", seq,
		"target", res.target,
		"time", res.rtt.Round(time.Millisecond),
	}
	if downtime > 0 {
		attrs = append(attrs, "downtime", downtime.Round(time.Millisecond))
	}
	return attrs
}

// setupAttrs builds the slog attr list for a "setup" line.
func setupAttrs(res probeResult) []any {
	return []any{
		"target", res.target,
		"dns", res.dns.Round(time.Millisecond),
		"tcp", res.tcp.Round(time.Millisecond),
		"tls", res.tls.Round(time.Millisecond),
	}
}

// classify maps a probe error to a short label, falling back to err.Error().
func classify(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused"
	case errors.Is(err, syscall.EHOSTUNREACH):
		return "host-unreachable"
	case errors.Is(err, syscall.ENETUNREACH):
		return "net-unreachable"
	}
	// AsType for the concrete *net.DNSError, classic As for the net.Error interface
	// (AsType requires a concrete type, so the timeout check has to use As).
	if _, ok := errors.AsType[*net.DNSError](err); ok {
		return "dns"
	}
	// covers context.DeadlineExceeded AND client.Timeout (which surfaces as
	// *url.Error wrapping a net timeout, not as context.DeadlineExceeded).
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return err.Error()
}

// pingHandler is a slog handler with millisecond timestamps in the legacy
// log.Default layout: positional time + level + msg, then key=val attrs.
type pingHandler struct {
	out io.Writer
	mu  sync.Mutex
}

// Enabled accepts every level.
func (h *pingHandler) Enabled(context.Context, slog.Level) bool { return true }

// Handle formats one record as: "<time> <LEVEL> <msg> key=val key=val\n".
func (h *pingHandler) Handle(_ context.Context, r slog.Record) error {
	var sb strings.Builder
	sb.WriteString(r.Time.Format("2006/01/02 15:04:05.000"))
	sb.WriteByte(' ')
	sb.WriteString(r.Level.String())
	sb.WriteByte(' ')
	sb.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		sb.WriteByte(' ')
		sb.WriteString(a.Key)
		sb.WriteByte('=')
		sb.WriteString(a.Value.String())
		return true
	})
	sb.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.out, sb.String())
	return err
}

// WithAttrs and WithGroup are no-ops.
func (h *pingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *pingHandler) WithGroup(string) slog.Handler      { return h }
