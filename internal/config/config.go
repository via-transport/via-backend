// Package config provides environment-based, immutable configuration for the gateway.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration. It is created once at startup and
// passed by value or pointer to every component that needs it. No component
// reads the environment directly.
type Config struct {
	// Server
	ListenAddr        string
	ReadHeaderTimeout time.Duration

	// NATS
	NATSURL           string
	NATSName          string
	NATSMaxReconnects int
	NATSReconnectWait time.Duration

	// JetStream – GPS_RAW stream
	GPSRawStreamName string
	GPSRawMaxAge     time.Duration

	// JetStream – GPS_SNAPSHOT KV
	GPSSnapshotBucket  string
	GPSSnapshotHistory int

	// GPS filtering (raw → live)
	GPSLiveMinDistance float64       // metres
	GPSLiveMinInterval time.Duration // fallback time gate

	// GPS cache bootstrap
	GPSBootstrapMaxAge time.Duration

	// WebSocket
	WSWriteTimeout  time.Duration
	WSReadTimeout   time.Duration
	WSPingInterval  time.Duration
	WSChannelBuffer int

	// Application cache
	CacheMaxItems        int
	CacheDefaultTTL      time.Duration
	CacheCleanupInterval time.Duration

	// Tracing (OpenTelemetry)
	TracingEnabled    bool
	TracingEndpoint   string
	TracingInsecure   bool
	TracingSampleRate float64

	// Sentry
	SentryEnabled          bool
	SentryDSN              string
	SentryTracesSampleRate float64
	SentryDebug            bool

	// Auth / RBAC
	AuthEnabled bool
	AuthAPIKeys string // comma-separated key:role:fleet_id entries

	// JWT
	JWTSecret     string
	JWTAccessTTL  time.Duration
	JWTRefreshTTL time.Duration
	JWTIssuer     string

	// General
	Environment string
	Release     string
	LogLevel    string
}

// Load reads environment variables and returns an immutable Config.
// Every field has a sensible default so the gateway works out-of-the-box.
func Load() Config {
	return Config{
		// Server
		ListenAddr:        envStr("LISTEN_ADDR", ":9090"),
		ReadHeaderTimeout: envDur("READ_HEADER_TIMEOUT", 5*time.Second),

		// NATS
		NATSURL:           envStr("NATS_URL", "nats://127.0.0.1:4222"),
		NATSName:          envStr("NATS_CLIENT_NAME", "via-realtime-gateway"),
		NATSMaxReconnects: envInt("NATS_MAX_RECONNECTS", -1),
		NATSReconnectWait: envDur("NATS_RECONNECT_WAIT", 2*time.Second),

		// JetStream
		GPSRawStreamName:   envStr("GPS_RAW_STREAM", "GPS_RAW"),
		GPSRawMaxAge:       envDur("GPS_RAW_MAX_AGE", 24*time.Hour),
		GPSSnapshotBucket:  envStr("GPS_SNAPSHOT_BUCKET", "GPS_SNAPSHOT"),
		GPSSnapshotHistory: envInt("GPS_SNAPSHOT_HISTORY", 1),

		// GPS filtering
		GPSLiveMinDistance: envFloat("GPS_LIVE_MIN_DISTANCE_M", 10.0),
		GPSLiveMinInterval: envDur("GPS_LIVE_MIN_INTERVAL", 30*time.Second),

		// Cache
		GPSBootstrapMaxAge:   envDur("GPS_BOOTSTRAP_MAX_AGE", 2*time.Minute),
		CacheMaxItems:        envInt("CACHE_MAX_ITEMS", 10000),
		CacheDefaultTTL:      envDur("CACHE_DEFAULT_TTL", 5*time.Minute),
		CacheCleanupInterval: envDur("CACHE_CLEANUP_INTERVAL", 1*time.Minute),

		// WebSocket
		WSWriteTimeout:  envDur("WS_WRITE_TIMEOUT", 5*time.Second),
		WSReadTimeout:   envDur("WS_READ_TIMEOUT", 120*time.Second),
		WSPingInterval:  envDur("WS_PING_INTERVAL", 40*time.Second),
		WSChannelBuffer: envInt("WS_CHANNEL_BUFFER", 256),

		// Tracing (OpenTelemetry)
		TracingEnabled:    envBool("TRACING_ENABLED", false),
		TracingEndpoint:   envStr("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		TracingInsecure:   envBool("OTEL_EXPORTER_OTLP_INSECURE", true),
		TracingSampleRate: envFloat("TRACING_SAMPLE_RATE", 0.1),

		// Sentry
		SentryEnabled:          envBool("SENTRY_ENABLED", false),
		SentryDSN:              envStr("SENTRY_DSN", ""),
		SentryTracesSampleRate: envFloat("SENTRY_TRACES_SAMPLE_RATE", 0.2),
		SentryDebug:            envBool("SENTRY_DEBUG", false),

		// Auth
		AuthEnabled: envBool("AUTH_ENABLED", false),
		AuthAPIKeys: envStr("AUTH_API_KEYS", ""),

		// JWT
		JWTSecret:     envStr("JWT_SECRET", "change-me-in-production"),
		JWTAccessTTL:  envDur("JWT_ACCESS_TTL", 1*time.Hour),
		JWTRefreshTTL: envDur("JWT_REFRESH_TTL", 7*24*time.Hour),
		JWTIssuer:     envStr("JWT_ISSUER", "via-backend"),

		// General
		Environment: envStr("ENVIRONMENT", "development"),
		Release:     envStr("RELEASE", "dev"),
		LogLevel:    envStr("LOG_LEVEL", "info"),
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func envStr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func envInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envFloat(key string, fallback float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return f
}

func envDur(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func envBool(key string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}
