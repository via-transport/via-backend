package logx

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/google/uuid"
)

type contextKey struct{}

var requestIDKey = contextKey{}

func Configure(level string) {
	slog.SetDefault(
		slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
				Level: parseLevel(level),
			}),
		),
	)
}

func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, strings.TrimSpace(requestID))
}

func RequestID(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDKey).(string)
	return strings.TrimSpace(requestID)
}

func RequestIDFromHeaderOrNew(headerValue string) string {
	normalized := strings.TrimSpace(headerValue)
	if normalized != "" {
		return normalized
	}
	return uuid.NewString()
}

func Logger(ctx context.Context) *slog.Logger {
	requestID := RequestID(ctx)
	if requestID == "" {
		return slog.Default()
	}
	return slog.Default().With("request_id", requestID)
}

func MaskEmail(email string) string {
	normalized := strings.TrimSpace(strings.ToLower(email))
	at := strings.Index(normalized, "@")
	if at <= 0 {
		if normalized == "" {
			return ""
		}
		return "***"
	}
	local := normalized[:at]
	domain := normalized[at+1:]
	if len(local) <= 2 {
		return "***@" + domain
	}
	return local[:1] + "***" + local[len(local)-1:] + "@" + domain
}

func parseLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
