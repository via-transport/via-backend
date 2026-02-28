// Package messaging provides a thin abstraction over NATS Core + JetStream.
// It owns the connection lifecycle, stream provisioning, and publishing.
package messaging

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"via-backend/internal/config"
)

// Broker wraps a NATS connection and the JetStream context.
type Broker struct {
	NC *nats.Conn
	JS jetstream.JetStream
}

// NewBroker connects to NATS and initialises JetStream.
func NewBroker(cfg config.Config) (*Broker, error) {
	nc, err := nats.Connect(
		cfg.NATSURL,
		nats.Name(cfg.NATSName),
		nats.MaxReconnects(cfg.NATSMaxReconnects),
		nats.ReconnectWait(cfg.NATSReconnectWait),
		nats.PingInterval(20*time.Second),
		nats.MaxPingsOutstanding(3),
		nats.ReconnectBufSize(16*1024*1024), // 16 MB
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Printf("[nats] disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Printf("[nats] reconnected to %s", c.ConnectedUrl())
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream init: %w", err)
	}

	return &Broker{NC: nc, JS: js}, nil
}

// ProvisionStreams creates (or updates) all JetStream streams and KV buckets
// required by the gateway. This is idempotent and safe to call on every startup.
func (b *Broker) ProvisionStreams(cfg config.Config) (jetstream.KeyValue, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// GPS_RAW durable stream for audit / replay.
	_, err := b.JS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        cfg.GPSRawStreamName,
		Description: "Raw GPS telemetry – retained for audit/debug",
		Subjects:    []string{"fleet.*.vehicle.*.gps.raw"},
		MaxAge:      cfg.GPSRawMaxAge,
		Storage:     jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("provision stream %s: %w", cfg.GPSRawStreamName, err)
	}

	// GPS_SNAPSHOT KV – latest point per vehicle for restart-safe bootstrap.
	kv, err := b.JS.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      cfg.GPSSnapshotBucket,
		Description: "Latest GPS position per vehicle",
		History:     uint8(cfg.GPSSnapshotHistory),
	})
	if err != nil {
		return nil, fmt.Errorf("provision KV %s: %w", cfg.GPSSnapshotBucket, err)
	}

	return kv, nil
}

// ProvisionKV creates or updates a single KV bucket. Returns the KV handle.
func (b *Broker) ProvisionKV(name, description string) (jetstream.KeyValue, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	kv, err := b.JS.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      name,
		Description: description,
		History:     1,
	})
	if err != nil {
		return nil, fmt.Errorf("provision KV %s: %w", name, err)
	}
	return kv, nil
}

// Conn returns the underlying NATS connection.
func (b *Broker) Conn() *nats.Conn {
	return b.NC
}

// Publish publishes a message on a core-NATS subject.
func (b *Broker) Publish(subject string, data []byte) error {
	return b.NC.Publish(subject, data)
}

// Subscribe creates a core-NATS subscription that pushes messages into ch.
// It returns the underlying subscription for later cleanup.
func (b *Broker) Subscribe(subject string, ch chan<- []byte) (*nats.Subscription, error) {
	return b.NC.Subscribe(subject, func(msg *nats.Msg) {
		select {
		case ch <- msg.Data:
		default:
			// Drop if consumer is too slow – back-pressure at the edge.
		}
	})
}

// Close drains and closes the NATS connection gracefully.
func (b *Broker) Close() {
	if b.NC != nil {
		_ = b.NC.Drain()
	}
}
