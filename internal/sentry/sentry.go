// Package sentry provides Sentry error tracking and performance monitoring.
// It wraps the official sentry-go SDK with gateway-specific defaults and
// exposes middleware for automatic panic capture and request tracing.
package sentry

import (
	"fmt"
	"log"
	"net/http"
	"time"

	gosentry "github.com/getsentry/sentry-go"
	sentryhttp "github.com/getsentry/sentry-go/http"
)

// Config holds Sentry configuration.
type Config struct {
	Enabled          bool
	DSN              string
	Environment      string
	Release          string
	SampleRate       float64 // error sample rate (0.0–1.0)
	TracesSampleRate float64 // performance/transaction rate
	Debug            bool
}

// Init initialises the Sentry SDK globally. Call Flush() before process exit.
func Init(cfg Config) error {
	if !cfg.Enabled || cfg.DSN == "" {
		log.Println("[sentry] disabled (no DSN)")
		return nil
	}

	err := gosentry.Init(gosentry.ClientOptions{
		Dsn:              cfg.DSN,
		Environment:      cfg.Environment,
		Release:          cfg.Release,
		SampleRate:       cfg.SampleRate,
		TracesSampleRate: cfg.TracesSampleRate,
		Debug:            cfg.Debug,
		EnableTracing:    cfg.TracesSampleRate > 0,
		AttachStacktrace: true,
		BeforeSend: func(event *gosentry.Event, hint *gosentry.EventHint) *gosentry.Event {
			// Scrub sensitive fields before sending to Sentry.
			if event.Request != nil {
				for k := range event.Request.Headers {
					if k == "Authorization" {
						event.Request.Headers[k] = "[REDACTED]"
					}
				}
			}
			return event
		},
	})
	if err != nil {
		return fmt.Errorf("sentry init: %w", err)
	}

	log.Printf("[sentry] enabled (env=%s, traces=%.2f)", cfg.Environment, cfg.TracesSampleRate)
	return nil
}

// Flush waits for buffered events to be sent.
func Flush(timeout time.Duration) {
	gosentry.Flush(timeout)
}

// CaptureError sends an error to Sentry with optional tags.
func CaptureError(err error, tags map[string]string) {
	if err == nil {
		return
	}
	hub := gosentry.CurrentHub().Clone()
	if len(tags) > 0 {
		hub.Scope().SetTags(tags)
	}
	hub.CaptureException(err)
}

// CaptureMessage sends an info-level message to Sentry.
func CaptureMessage(msg string) {
	gosentry.CaptureMessage(msg)
}

// HTTPMiddleware returns Sentry's HTTP handler that automatically:
// - Captures panics as Sentry events with full stack traces
// - Creates transactions for performance monitoring
// - Attaches request context (URL, method, headers)
func HTTPMiddleware() func(http.Handler) http.Handler {
	handler := sentryhttp.New(sentryhttp.Options{
		Repanic:         true, // re-panic after capture so our Recovery middleware also fires
		WaitForDelivery: false,
		Timeout:         2 * time.Second,
	})
	return func(next http.Handler) http.Handler {
		return handler.Handle(next)
	}
}

// SetUser attaches user identity to the current Sentry scope.
// Call from authenticated handlers.
func SetUser(r *http.Request, id, email, role string) {
	if hub := gosentry.GetHubFromContext(r.Context()); hub != nil {
		hub.Scope().SetUser(gosentry.User{
			ID:    id,
			Email: email,
		})
		hub.Scope().SetTag("role", role)
	}
}

// AddBreadcrumb adds a navigation/action breadcrumb to the current scope.
func AddBreadcrumb(r *http.Request, category, message string) {
	if hub := gosentry.GetHubFromContext(r.Context()); hub != nil {
		hub.AddBreadcrumb(&gosentry.Breadcrumb{
			Category: category,
			Message:  message,
			Level:    gosentry.LevelInfo,
		}, nil)
	}
}
