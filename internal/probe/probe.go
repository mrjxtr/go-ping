// Package probe
package probe

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"syscall"
	"time"
)

const userAgent = "go-ping/1.0"

// probeResult is the timing breakdown for one successful probe.
type ProbeResult struct {
	Target string
	RTT    time.Duration
	DNS    time.Duration
	TCP    time.Duration
	TLS    time.Duration
}

// cold reports whether this probe had to dial fresh (DNS/TCP/TLS were paid).
func (r ProbeResult) Cold() bool { return r.DNS > 0 || r.TCP > 0 || r.TLS > 0 }

// probe tries each url in order and returns on the first 204; on full failure
// returns the last error.
func Probe(
	ctx context.Context,
	client *http.Client,
	urls []string,
) (bool, ProbeResult, error) {
	var lastErr error
	for _, probeURL := range urls {
		if ctx.Err() != nil {
			return false, ProbeResult{}, ctx.Err()
		}
		res, err := check(ctx, client, probeURL)
		if err == nil {
			return true, res, nil
		}
		lastErr = err
	}
	return false, ProbeResult{}, lastErr
}

// check fires one request and returns the timing breakdown on a 204; any other
// status is a failure (likely captive portal). RTT is "request written → first
// response byte" via httptrace; setup phases are filled only on a fresh dial.
func check(ctx context.Context, client *http.Client, url string) (ProbeResult, error) {
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
		return ProbeResult{}, err
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return ProbeResult{}, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusNoContent {
		return ProbeResult{}, fmt.Errorf(
			"unexpected status %d (possible captive portal)",
			resp.StatusCode,
		)
	}

	res := ProbeResult{Target: url, RTT: firstByteAt.Sub(wroteAt)}
	if !reused {
		// guard each phase: a cached DNS lookup may not fire DNSStart, and an
		// http (not https) request never fires the TLS hooks at all.
		if !dnsStart.IsZero() && !dnsDone.IsZero() {
			res.DNS = dnsDone.Sub(dnsStart)
		}
		if !connStart.IsZero() && !connDone.IsZero() {
			res.TCP = connDone.Sub(connStart)
		}
		if !tlsStart.IsZero() && !tlsDone.IsZero() {
			res.TLS = tlsDone.Sub(tlsStart)
		}
	}
	return res, nil
}

// probeAttrs builds the slog attr list for an "online" log line.
func ProbeAttrs(seq uint64, res ProbeResult, downtime time.Duration) []any {
	attrs := []any{
		"seq", seq,
		"target", res.Target,
		"time", res.RTT.Round(time.Millisecond),
	}
	if downtime > 0 {
		attrs = append(attrs, "downtime", downtime.Round(time.Millisecond))
	}
	return attrs
}

// setupAttrs builds the slog attr list for a "setup" line.
func SetupAttrs(res ProbeResult) []any {
	return []any{
		"target", res.Target,
		"dns", res.DNS.Round(time.Millisecond),
		"tcp", res.TCP.Round(time.Millisecond),
		"tls", res.TLS.Round(time.Millisecond),
	}
}

// classify maps a probe error to a short label, falling back to err.Error().
func Classify(err error) string {
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
