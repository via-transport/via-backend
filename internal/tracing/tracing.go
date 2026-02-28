// Package tracing provides OpenTelemetry-based distributed tracing.
// It initialises a TracerProvider that can export to OTLP (Jaeger, Tempo,
// Grafana Cloud, etc.) or stdout for local development.
package tracing

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Config holds tracing configuration.
type Config struct {
	Enabled     bool
	ServiceName string
	Environment string
	Endpoint    string // OTLP HTTP endpoint, e.g. "localhost:4318"
	Insecure    bool   // use HTTP instead of HTTPS
	SampleRate  float64
}

// Provider wraps the OTel TracerProvider and exposes a clean shutdown.
type Provider struct {
	tp *sdktrace.TracerProvider
}

// Init creates and registers a global TracerProvider.
// Call Shutdown() on the returned Provider before process exit.
func Init(cfg Config) (*Provider, error) {
	if !cfg.Enabled {
		// No-op provider – all spans are dropped, zero overhead.
		noop := sdktrace.NewTracerProvider()
		otel.SetTracerProvider(noop)
		return &Provider{tp: noop}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
			semconv.DeploymentEnvironmentKey.String(cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing resource: %w", err)
	}

	var exporter sdktrace.SpanExporter
	if cfg.Endpoint != "" {
		opts := []otlptracehttp.Option{
			otlptracehttp.WithEndpoint(cfg.Endpoint),
		}
		if cfg.Insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		exporter, err = otlptracehttp.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("otlp exporter: %w", err)
		}
	} else {
		// Fallback: stdout exporter for local dev.
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("stdout exporter: %w", err)
		}
	}

	sampler := sdktrace.TraceIDRatioBased(cfg.SampleRate)
	if cfg.SampleRate >= 1.0 {
		sampler = sdktrace.AlwaysSample()
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.Printf("[tracing] enabled (endpoint=%s, sample_rate=%.2f)", cfg.Endpoint, cfg.SampleRate)
	return &Provider{tp: tp}, nil
}

// Shutdown flushes pending spans and releases resources.
func (p *Provider) Shutdown(ctx context.Context) {
	if p.tp != nil {
		if err := p.tp.Shutdown(ctx); err != nil {
			log.Printf("[tracing] shutdown: %v", err)
		}
	}
}

// Tracer returns a named tracer from the global provider.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

// SpanFromContext extracts the current span from context.
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// AddEvent adds an event to the current span.
func AddEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	trace.SpanFromContext(ctx).AddEvent(name, trace.WithAttributes(attrs...))
}
