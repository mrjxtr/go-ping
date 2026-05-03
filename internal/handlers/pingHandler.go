// Package handlers
package handlers

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
)

// PingHandler is a slog handler with millisecond timestamps in the legacy
// log.Default layout: positional time + level + msg, then key=val attrs.
type PingHandler struct {
	Out io.Writer
	Mu  sync.Mutex
}

// Enabled accepts every level.
func (h *PingHandler) Enabled(context.Context, slog.Level) bool { return true }

// Handle formats one record as: "<time> <LEVEL> <msg> key=val key=val\n".
func (h *PingHandler) Handle(_ context.Context, r slog.Record) error {
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

	h.Mu.Lock()
	defer h.Mu.Unlock()
	_, err := io.WriteString(h.Out, sb.String())
	return err
}

// WithAttrs and WithGroup are no-ops.
func (h *PingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *PingHandler) WithGroup(string) slog.Handler      { return h }
