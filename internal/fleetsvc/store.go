package fleetsvc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"via-backend/internal/appcache"
)

// maxConcurrentKV limits parallel KV gets to avoid overwhelming NATS.
const maxConcurrentKV = 16

const cacheTTL = 2 * time.Minute // per-entity cache TTL

// Store provides NATS KV backed persistence for fleet data.
type Store struct {
	vehicles jetstream.KeyValue // key: {fleet_id}.{vehicle_id}
	drivers  jetstream.KeyValue // key: {fleet_id}.{driver_id}
	events   jetstream.KeyValue // key: {event_id}
	notices  jetstream.KeyValue // key: {notice_id}
	cache    *appcache.Cache    // optional in-memory cache
}

// NewStore creates a fleet store. Pass nil for cache to disable caching.
func NewStore(vehicles, drivers, events, notices jetstream.KeyValue, cache *appcache.Cache) *Store {
	return &Store{
		vehicles: vehicles,
		drivers:  drivers,
		events:   events,
		notices:  notices,
		cache:    cache,
	}
}

// ---------------------------------------------------------------------------
// Vehicles
// ---------------------------------------------------------------------------

func vehicleKey(fleetID, vehicleID string) string {
	return fleetID + "." + vehicleID
}

func (s *Store) PutVehicle(ctx context.Context, v *Vehicle) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	key := vehicleKey(v.FleetID, v.ID)
	_, err = s.vehicles.Put(ctx, key, data)
	if err == nil && s.cache != nil {
		s.cache.SetWithTTL("v:"+key, v, cacheTTL)
	}
	return err
}

func (s *Store) GetVehicle(ctx context.Context, fleetID, vehicleID string) (*Vehicle, error) {
	key := vehicleKey(fleetID, vehicleID)
	if s.cache != nil {
		if cached, ok := s.cache.Get("v:" + key); ok {
			return cached.(*Vehicle), nil
		}
	}
	entry, err := s.vehicles.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("vehicle not found: %w", err)
	}
	var v Vehicle
	if err := json.Unmarshal(entry.Value(), &v); err != nil {
		return nil, err
	}
	if s.cache != nil {
		s.cache.SetWithTTL("v:"+key, &v, cacheTTL)
	}
	return &v, nil
}

func (s *Store) DeleteVehicle(ctx context.Context, fleetID, vehicleID string) error {
	key := vehicleKey(fleetID, vehicleID)
	if s.cache != nil {
		s.cache.Delete("v:" + key)
	}
	return s.vehicles.Delete(ctx, key)
}

// GetVehicleByID looks up a vehicle by ID across all fleets.
// Uses key suffix matching instead of fetching all values.
func (s *Store) GetVehicleByID(ctx context.Context, vehicleID string) (*Vehicle, error) {
	keys, err := s.vehicles.Keys(ctx)
	if err != nil {
		return nil, fmt.Errorf("vehicle not found: %w", err)
	}
	suffix := "." + vehicleID
	for _, k := range keys {
		if strings.HasSuffix(k, suffix) {
			entry, err := s.vehicles.Get(ctx, k)
			if err != nil {
				continue
			}
			var v Vehicle
			if err := json.Unmarshal(entry.Value(), &v); err != nil {
				continue
			}
			return &v, nil
		}
	}
	return nil, fmt.Errorf("vehicle %s not found in any fleet", vehicleID)
}

func (s *Store) ListVehicles(ctx context.Context, fleetID string) ([]Vehicle, error) {
	keys, err := s.vehicles.Keys(ctx)
	if err != nil {
		if isNoKeys(err) {
			return nil, nil
		}
		return nil, err
	}
	prefix := fleetID + "."
	var filtered []string
	for _, k := range keys {
		if fleetID != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		filtered = append(filtered, k)
	}
	return fetchConcurrent(ctx, s.vehicles, filtered, func(data []byte) (Vehicle, bool) {
		var v Vehicle
		if json.Unmarshal(data, &v) != nil {
			return v, false
		}
		return v, true
	})
}

func (s *Store) ListVehiclesForDriver(ctx context.Context, fleetID, driverID string) ([]Vehicle, error) {
	all, err := s.ListVehicles(ctx, fleetID)
	if err != nil {
		return nil, err
	}
	var result []Vehicle
	for _, v := range all {
		if v.DriverID == driverID {
			result = append(result, v)
		}
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Drivers
// ---------------------------------------------------------------------------

func driverKey(fleetID, driverID string) string {
	return fleetID + "." + driverID
}

func (s *Store) PutDriver(ctx context.Context, d *Driver) error {
	data, err := json.Marshal(d)
	if err != nil {
		return err
	}
	key := driverKey(d.FleetID, d.ID)
	_, err = s.drivers.Put(ctx, key, data)
	if err == nil && s.cache != nil {
		s.cache.SetWithTTL("d:"+key, d, cacheTTL)
	}
	return err
}

func (s *Store) GetDriver(ctx context.Context, fleetID, driverID string) (*Driver, error) {
	key := driverKey(fleetID, driverID)
	if s.cache != nil {
		if cached, ok := s.cache.Get("d:" + key); ok {
			return cached.(*Driver), nil
		}
	}
	entry, err := s.drivers.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("driver not found: %w", err)
	}
	var d Driver
	if err := json.Unmarshal(entry.Value(), &d); err != nil {
		return nil, err
	}
	if s.cache != nil {
		s.cache.SetWithTTL("d:"+key, &d, cacheTTL)
	}
	return &d, nil
}

func (s *Store) DeleteDriver(ctx context.Context, fleetID, driverID string) error {
	key := driverKey(fleetID, driverID)
	if s.cache != nil {
		s.cache.Delete("d:" + key)
	}
	return s.drivers.Delete(ctx, key)
}

func (s *Store) ListDrivers(ctx context.Context, fleetID string) ([]Driver, error) {
	keys, err := s.drivers.Keys(ctx)
	if err != nil {
		if isNoKeys(err) {
			return nil, nil
		}
		return nil, err
	}
	prefix := fleetID + "."
	var filtered []string
	for _, k := range keys {
		if fleetID != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		filtered = append(filtered, k)
	}
	return fetchConcurrent(ctx, s.drivers, filtered, func(data []byte) (Driver, bool) {
		var d Driver
		if json.Unmarshal(data, &d) != nil {
			return d, false
		}
		return d, true
	})
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

func (s *Store) PutEvent(ctx context.Context, e *SpecialEvent) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = s.events.Put(ctx, e.ID, data)
	return err
}

func (s *Store) GetEvent(ctx context.Context, eventID string) (*SpecialEvent, error) {
	entry, err := s.events.Get(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("event not found: %w", err)
	}
	var e SpecialEvent
	if err := json.Unmarshal(entry.Value(), &e); err != nil {
		return nil, err
	}
	return &e, nil
}

func (s *Store) ListEvents(ctx context.Context, fleetID, vehicleID string) ([]SpecialEvent, error) {
	keys, err := s.events.Keys(ctx)
	if err != nil {
		if isNoKeys(err) {
			return nil, nil
		}
		return nil, err
	}
	return fetchConcurrent(ctx, s.events, keys, func(data []byte) (SpecialEvent, bool) {
		var e SpecialEvent
		if json.Unmarshal(data, &e) != nil {
			return e, false
		}
		if fleetID != "" && e.FleetID != fleetID {
			return e, false
		}
		if vehicleID != "" && e.VehicleID != vehicleID {
			return e, false
		}
		return e, true
	})
}

// ---------------------------------------------------------------------------
// Notices
// ---------------------------------------------------------------------------

func (s *Store) PutNotice(ctx context.Context, n *DriverNotice) error {
	data, err := json.Marshal(n)
	if err != nil {
		return err
	}
	_, err = s.notices.Put(ctx, n.ID, data)
	return err
}

func (s *Store) GetNotice(ctx context.Context, noticeID string) (*DriverNotice, error) {
	entry, err := s.notices.Get(ctx, noticeID)
	if err != nil {
		return nil, fmt.Errorf("notice not found: %w", err)
	}
	var n DriverNotice
	if err := json.Unmarshal(entry.Value(), &n); err != nil {
		return nil, err
	}
	return &n, nil
}

func (s *Store) ListNotices(ctx context.Context, fleetID, vehicleID, driverID string) ([]DriverNotice, error) {
	keys, err := s.notices.Keys(ctx)
	if err != nil {
		if isNoKeys(err) {
			return nil, nil
		}
		return nil, err
	}
	return fetchConcurrent(ctx, s.notices, keys, func(data []byte) (DriverNotice, bool) {
		var n DriverNotice
		if json.Unmarshal(data, &n) != nil {
			return n, false
		}
		if fleetID != "" && n.FleetID != fleetID {
			return n, false
		}
		if vehicleID != "" && n.VehicleID != vehicleID {
			return n, false
		}
		if driverID != "" && n.DriverID != driverID {
			return n, false
		}
		return n, true
	})
}

// fetchConcurrent reads multiple keys from a KV bucket concurrently with
// bounded parallelism. The accept function deserialises and filters; return
// false to skip the entry.
func fetchConcurrent[T any](ctx context.Context, kv jetstream.KeyValue, keys []string, accept func([]byte) (T, bool)) ([]T, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	type indexed struct {
		idx int
		val T
	}

	results := make([]indexed, 0, len(keys))
	var mu sync.Mutex
	sem := make(chan struct{}, maxConcurrentKV)
	var wg sync.WaitGroup

	for i, k := range keys {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, key string) {
			defer func() { <-sem; wg.Done() }()
			entry, err := kv.Get(ctx, key)
			if err != nil {
				return
			}
			if v, ok := accept(entry.Value()); ok {
				mu.Lock()
				results = append(results, indexed{idx: idx, val: v})
				mu.Unlock()
			}
		}(i, k)
	}
	wg.Wait()

	// Preserve original key order.
	out := make([]T, 0, len(results))
	// Sort by index (simple insertion for typically small slices).
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].idx < results[i].idx {
				results[i], results[j] = results[j], results[i]
			}
		}
	}
	for _, r := range results {
		out = append(out, r.val)
	}
	return out, nil
}

func isNoKeys(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no keys found")
}
