// Package cache provides a thread-safe in-memory GPS position cache
// that can be seeded from JetStream KV on startup.
package cache

import (
	"encoding/json"
	"sync"
	"time"

	"via-backend/internal/model"
)

// GPSCache stores the latest GPS position per vehicle, keyed by
// fleet → vehicle. It is used for WebSocket bootstrap so newly connected
// clients immediately see the most recent known positions.
type GPSCache struct {
	mu      sync.RWMutex
	byFleet map[string]map[string]model.GPSUpdate
	maxAge  time.Duration
}

// New creates a GPSCache with the given bootstrap max-age window.
func New(maxAge time.Duration) *GPSCache {
	return &GPSCache{
		byFleet: make(map[string]map[string]model.GPSUpdate),
		maxAge:  maxAge,
	}
}

// Store upserts a GPS point. Out-of-order (older) updates are silently dropped.
func (c *GPSCache) Store(u model.GPSUpdate) {
	c.mu.Lock()
	defer c.mu.Unlock()

	fleet := c.byFleet[u.FleetID]
	if fleet == nil {
		fleet = make(map[string]model.GPSUpdate)
		c.byFleet[u.FleetID] = fleet
	}

	if existing, ok := fleet[u.VehicleID]; ok {
		if !existing.Timestamp.IsZero() && u.Timestamp.Before(existing.Timestamp) {
			return
		}
	}
	fleet[u.VehicleID] = u
}

// GetLast returns the latest cached point for a single vehicle.
func (c *GPSCache) GetLast(fleetID, vehicleID string) (model.GPSUpdate, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	fleet := c.byFleet[fleetID]
	if fleet == nil {
		return model.GPSUpdate{}, false
	}
	u, ok := fleet[vehicleID]
	return u, ok
}

// Snapshot returns JSON-encoded payloads of all positions within the
// bootstrap window for the given fleet (optionally filtered to one vehicle).
func (c *GPSCache) Snapshot(fleetID, vehicleID string) [][]byte {
	cutoff := time.Now().UTC().Add(-c.maxAge)

	c.mu.RLock()
	defer c.mu.RUnlock()

	fleet := c.byFleet[fleetID]
	if len(fleet) == 0 {
		return nil
	}

	var updates []model.GPSUpdate
	if vehicleID != "" {
		u, ok := fleet[vehicleID]
		if !ok || u.Timestamp.Before(cutoff) {
			return nil
		}
		updates = append(updates, u)
	} else {
		for _, u := range fleet {
			if u.Timestamp.Before(cutoff) {
				continue
			}
			updates = append(updates, u)
		}
	}

	payloads := make([][]byte, 0, len(updates))
	for _, u := range updates {
		b, err := json.Marshal(u)
		if err != nil {
			continue
		}
		payloads = append(payloads, b)
	}
	return payloads
}

// Seed bulk-loads positions (typically from JetStream KV at startup).
func (c *GPSCache) Seed(updates []model.GPSUpdate) {
	for _, u := range updates {
		c.Store(u)
	}
}
