// Package service implements the business logic for GPS and event processing.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"via-backend/internal/cache"
	"via-backend/internal/config"
	"via-backend/internal/geo"
	"via-backend/internal/messaging"
	"via-backend/internal/model"
)

// GPSService handles GPS ingestion, raw archival, live filtering,
// KV snapshotting, and cache management.
type GPSService struct {
	broker *messaging.Broker
	cache  *cache.GPSCache
	kv     jetstream.KeyValue
	cfg    config.Config
}

// NewGPSService creates a GPSService with all its dependencies injected.
func NewGPSService(
	broker *messaging.Broker,
	gpsCache *cache.GPSCache,
	kv jetstream.KeyValue,
	cfg config.Config,
) *GPSService {
	return &GPSService{
		broker: broker,
		cache:  gpsCache,
		kv:     kv,
		cfg:    cfg,
	}
}

// SeedCache reads every key from the GPS_SNAPSHOT KV and populates the
// in-memory cache. Safe to call once at startup.
func (s *GPSService) SeedCache(ctx context.Context) {
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		// ErrNoKeysFound is expected on a fresh deployment.
		if !isNoKeysErr(err) {
			log.Printf("[gps] warning: could not seed cache from KV: %v", err)
		}
		return
	}

	loaded := 0
	for _, k := range keys {
		entry, err := s.kv.Get(ctx, k)
		if err != nil {
			continue
		}
		var p model.GPSUpdate
		if json.Unmarshal(entry.Value(), &p) == nil {
			s.cache.Store(p)
			loaded++
		}
	}
	log.Printf("[gps] seeded cache with %d positions from KV", loaded)
}

// IngestResult is the return value of Ingest.
type IngestResult struct {
	Subject string
}

// Ingest validates a GPS update, publishes it to the RAW stream, and
// conditionally publishes it to the LIVE fanout subject + KV snapshot.
func (s *GPSService) Ingest(ctx context.Context, p model.GPSUpdate) (IngestResult, error) {
	p.FleetID = strings.TrimSpace(p.FleetID)
	p.VehicleID = strings.TrimSpace(p.VehicleID)

	if p.FleetID == "" || p.VehicleID == "" {
		return IngestResult{}, fmt.Errorf("fleet_id and vehicle_id are required")
	}
	if p.Timestamp.IsZero() {
		p.Timestamp = time.Now().UTC()
	}

	body, err := json.Marshal(p)
	if err != nil {
		return IngestResult{}, fmt.Errorf("marshal payload: %w", err)
	}

	// 1. Always publish to RAW (durable JetStream stream).
	rawSub := messaging.GPSRawSubject(p.FleetID, p.VehicleID)
	if err := s.broker.Publish(rawSub, body); err != nil {
		return IngestResult{}, fmt.Errorf("publish raw: %w", err)
	}

	// 2. Decide whether this point should be published to LIVE.
	if s.shouldPublishLive(p) {
		liveSub := messaging.GPSLiveSubject(p.FleetID, p.VehicleID)
		_ = s.broker.Publish(liveSub, body) // best-effort for UI

		// 3. Persist latest point in KV for restart-safe recovery.
		kvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		key := fmt.Sprintf("%s_%s", p.FleetID, p.VehicleID)
		if _, err := s.kv.Put(kvCtx, key, body); err != nil {
			log.Printf("[gps] kv put warning: %v", err)
		}

		// 4. Update in-memory cache.
		s.cache.Store(p)
	}

	return IngestResult{Subject: rawSub}, nil
}

// Cache exposes the underlying GPS cache for the WebSocket handler.
func (s *GPSService) Cache() *cache.GPSCache {
	return s.cache
}

// shouldPublishLive applies distance and time gates so the LIVE stream is
// smooth but not noisy.
func (s *GPSService) shouldPublishLive(p model.GPSUpdate) bool {
	last, ok := s.cache.GetLast(p.FleetID, p.VehicleID)
	if !ok {
		return true // first point ever seen → always publish
	}
	dist := geo.HaversineDistance(last.Latitude, last.Longitude, p.Latitude, p.Longitude)
	elapsed := p.Timestamp.Sub(last.Timestamp)

	return dist >= s.cfg.GPSLiveMinDistance || elapsed >= s.cfg.GPSLiveMinInterval
}

// isNoKeysErr checks for JetStream "no keys found" which is expected on
// first run.
func isNoKeysErr(err error) bool {
	// jetstream.ErrNoKeysFound is the canonical sentinel.
	return err != nil && strings.Contains(err.Error(), "no keys found")
}
