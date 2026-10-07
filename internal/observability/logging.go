// Package observability provides JSON logging with contextual identifiers
// and Prometheus metrics.
package observability

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

type ctxKey struct{}

// Field names propagated through the context and added to every log record.
const (
	FieldCorrelationID = "correlationId"
	FieldMessageID     = "messageId"
	FieldTransactionID = "transactionId"
	FieldWalletID      = "walletId"
	FieldProviderID    = "providerId"
)

// WithFields returns a context carrying additional log attributes.
func WithFields(ctx context.Context, attrs ...slog.Attr) context.Context {
	prev, _ := ctx.Value(ctxKey{}).([]slog.Attr)
	merged := make([]slog.Attr, 0, len(prev)+len(attrs))
	merged = append(merged, prev...)
	merged = append(merged, attrs...)
	return context.WithValue(ctx, ctxKey{}, merged)
}

// CorrelationID returns the correlation id stored in the context, if any.
func CorrelationID(ctx context.Context) string {
	attrs, _ := ctx.Value(ctxKey{}).([]slog.Attr)
	for i := len(attrs) - 1; i >= 0; i-- {
		if attrs[i].Key == FieldCorrelationID {
			return attrs[i].Value.String()
		}
	}
	return ""
}

type contextHandler struct{ slog.Handler }

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if attrs, ok := ctx.Value(ctxKey{}).([]slog.Attr); ok {
		seen := map[string]bool{}
		r.Attrs(func(a slog.Attr) bool { seen[a.Key] = true; return true })
		for _, a := range attrs {
			if !seen[a.Key] {
				r.AddAttrs(a)
				seen[a.Key] = true
			}
		}
	}
	return h.Handler.Handle(ctx, r)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(name)}
}

// NewLogger builds the JSON logger. Credentials and full financial payloads
// are never logged by the code base; Authorization headers are not read by
// the logging middleware.
func NewLogger(level, instanceID string) *slog.Logger {
	return NewLoggerTo(os.Stdout, level, instanceID)
}

// NewLoggerTo is NewLogger with a custom writer.
func NewLoggerTo(w io.Writer, level, instanceID string) *slog.Logger {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lv})
	return slog.New(contextHandler{h}).With(slog.String("instance", instanceID), slog.String("service", "jungle-test"))
}
