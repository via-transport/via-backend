package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	defaultListenAddr  = ":9090"
	defaultNATSURL     = "nats://127.0.0.1:4222"
	gpsBootstrapMaxAge = 2 * time.Minute
	topicGPS           = "gps"
	topicTrip          = "trip"
	topicOps           = "ops"
	topicEvents        = "events"
)

type gpsUpdate struct {
	FleetID   string    `json:"fleet_id"`
	VehicleID string    `json:"vehicle_id"`
	DriverID  string    `json:"driver_id,omitempty"`
	RouteID   string    `json:"route_id,omitempty"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	SpeedKPH  float64   `json:"speed_kph,omitempty"`
	Heading   float64   `json:"heading,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

type realtimeEvent struct {
	FleetID   string    `json:"fleet_id"`
	VehicleID string    `json:"vehicle_id"`
	DriverID  string    `json:"driver_id,omitempty"`
	RouteID   string    `json:"route_id,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	Message   string    `json:"message,omitempty"`
	Event     string    `json:"event,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

type publishAck struct {
	Status  string `json:"status"`
	Subject string `json:"subject"`
}

type gpsCache struct {
	mu      sync.RWMutex
	byFleet map[string]map[string]gpsUpdate
}

func newGPSCache() *gpsCache {
	return &gpsCache{
		byFleet: make(map[string]map[string]gpsUpdate),
	}
}

func (c *gpsCache) Store(update gpsUpdate) {
	c.mu.Lock()
	defer c.mu.Unlock()

	fleetEntries := c.byFleet[update.FleetID]
	if fleetEntries == nil {
		fleetEntries = make(map[string]gpsUpdate)
		c.byFleet[update.FleetID] = fleetEntries
	}

	if existing, ok := fleetEntries[update.VehicleID]; ok {
		if !existing.Timestamp.IsZero() && update.Timestamp.Before(existing.Timestamp) {
			return
		}
	}

	fleetEntries[update.VehicleID] = update
}

func (c *gpsCache) Snapshot(fleetID, vehicleID string) [][]byte {
	cutoff := time.Now().UTC().Add(-gpsBootstrapMaxAge)

	c.mu.RLock()
	defer c.mu.RUnlock()

	fleetEntries := c.byFleet[fleetID]
	if len(fleetEntries) == 0 {
		return nil
	}

	updates := make([]gpsUpdate, 0, len(fleetEntries))
	if vehicleID != "" {
		update, ok := fleetEntries[vehicleID]
		if !ok || update.Timestamp.Before(cutoff) {
			return nil
		}
		updates = append(updates, update)
	} else {
		for _, update := range fleetEntries {
			if update.Timestamp.Before(cutoff) {
				continue
			}
			updates = append(updates, update)
		}
	}

	payloads := make([][]byte, 0, len(updates))
	for _, update := range updates {
		body, err := json.Marshal(update)
		if err != nil {
			continue
		}
		payloads = append(payloads, body)
	}
	return payloads
}

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(_ *http.Request) bool {
		return true
	},
}

func main() {
	listenAddr := envOrDefault("LISTEN_ADDR", defaultListenAddr)
	natsURL := envOrDefault("NATS_URL", defaultNATSURL)

	nc, err := nats.Connect(
		natsURL,
		nats.Name("via-gps-gateway"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("nats connect failed: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("jetstream initialization failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        "GPS_RAW",
		Description: "Raw GPS telemetry stream",
		Subjects:    []string{"fleet.*.vehicle.*.gps.raw"},
		MaxAge:      24 * time.Hour,
		Storage:     jetstream.FileStorage,
	})
	if err != nil {
		log.Fatalf("failed to create JetStream stream GPS_RAW: %v", err)
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        "EVENT_WINDOW",
		Description: "Realtime fleet events retained in a moving replay window",
		Subjects: []string{
			"fleet.*.events.>",
			"fleet.*.vehicle.*.trip.*",
			"fleet.*.vehicle.*.ops.>",
		},
		MaxAge:  6 * time.Hour,
		Storage: jetstream.FileStorage,
	})
	if err != nil {
		log.Fatalf("failed to create JetStream stream EVENT_WINDOW: %v", err)
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      "GPS_SNAPSHOT",
		Description: "Latest GPS positions per vehicle",
		History:     1,
	})
	if err != nil {
		log.Fatalf("failed to create JetStream KV GPS_SNAPSHOT: %v", err)
	}

	cache := newGPSCache()

	// Seed cache from KV
	keys, err := kv.Keys(ctx)
	if err == nil {
		for _, k := range keys {
			entry, err := kv.Get(ctx, k)
			if err == nil {
				var p gpsUpdate
				if json.Unmarshal(entry.Value(), &p) == nil {
					cache.Store(p)
				}
			}
		}
	} else if !errors.Is(err, jetstream.ErrNoKeysFound) {
		log.Printf("warning: could not read KV keys for seeding: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/v1/gps/update", gpsIngestHandler(nc, cache, kv))
	mux.HandleFunc("/v1/trip/start", tripStartIngestHandler(nc))
	mux.HandleFunc("/v1/events/publish", eventIngestHandler(nc))
	mux.HandleFunc("/ws", wsFanoutHandler(nc, cache))

	server := &http.Server{
		Addr:              listenAddr,
		Handler:           loggingMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("gps gateway listening on %s (nats: %s)", listenAddr, natsURL)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server failed: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown warning: %v", err)
	}
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (c *gpsCache) GetLast(fleetID, vehicleID string) (gpsUpdate, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	fleetEntries := c.byFleet[fleetID]
	if fleetEntries == nil {
		return gpsUpdate{}, false
	}
	update, ok := fleetEntries[vehicleID]
	return update, ok
}

func gpsIngestHandler(nc *nats.Conn, cache *gpsCache, kv jetstream.KeyValue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var payload gpsUpdate
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json payload")
			return
		}

		payload.FleetID = strings.TrimSpace(payload.FleetID)
		payload.VehicleID = strings.TrimSpace(payload.VehicleID)
		if payload.FleetID == "" || payload.VehicleID == "" {
			writeError(w, http.StatusBadRequest, "fleet_id and vehicle_id are required")
			return
		}
		if payload.Timestamp.IsZero() {
			payload.Timestamp = time.Now().UTC()
		}

		body, err := json.Marshal(payload)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "encode payload failed")
			return
		}

		// Always publish to RAW stream
		rawSub := gpsRawSubject(payload.FleetID, payload.VehicleID)
		if err := nc.Publish(rawSub, body); err != nil {
			writeError(w, http.StatusBadGateway, "nats raw publish failed")
			return
		}

		// Filter for LIVE fanout
		shouldPublishLive := false
		last, ok := cache.GetLast(payload.FleetID, payload.VehicleID)
		if !ok {
			shouldPublishLive = true
		} else {
			dist := haversineDistance(last.Latitude, last.Longitude, payload.Latitude, payload.Longitude)
			timeDiff := payload.Timestamp.Sub(last.Timestamp)
			// Smooth stream to UI: publish if > 10 meters OR if > 30 seconds have passed
			if dist >= 10.0 || timeDiff >= 30*time.Second {
				shouldPublishLive = true
			}
		}

		if shouldPublishLive {
			liveSub := gpsLiveSubject(payload.FleetID, payload.VehicleID)
			nc.Publish(liveSub, body)

			// Store in KV and Memory
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			key := fmt.Sprintf("%s_%s", payload.FleetID, payload.VehicleID)
			kv.Put(ctx, key, body)
			cache.Store(payload)
		}

		writeJSON(w, http.StatusAccepted, publishAck{
			Status:  "published",
			Subject: rawSub, // tell sender we received it at least
		})
	}
}

func tripStartIngestHandler(nc *nats.Conn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var payload realtimeEvent
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json payload")
			return
		}

		payload.Topic = topicTrip
		if strings.TrimSpace(payload.Event) == "" {
			payload.Event = "trip_started"
		}
		publishRealtimeEvent(w, nc, payload)
	}
}

func eventIngestHandler(nc *nats.Conn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var payload realtimeEvent
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json payload")
			return
		}

		publishRealtimeEvent(w, nc, payload)
	}
}

func publishRealtimeEvent(w http.ResponseWriter, nc *nats.Conn, payload realtimeEvent) {
	payload.FleetID = strings.TrimSpace(payload.FleetID)
	payload.VehicleID = strings.TrimSpace(payload.VehicleID)
	payload.DriverID = strings.TrimSpace(payload.DriverID)
	payload.RouteID = strings.TrimSpace(payload.RouteID)
	payload.Topic = strings.ToLower(strings.TrimSpace(payload.Topic))
	payload.Message = strings.TrimSpace(payload.Message)
	payload.Event = strings.TrimSpace(payload.Event)

	if payload.FleetID == "" {
		writeError(w, http.StatusBadRequest, "fleet_id is required")
		return
	}

	payload.Topic = normalizeRealtimeTopic(payload.Topic, payload.Event)
	if payload.VehicleID == "" && payload.Topic != topicOps {
		writeError(w, http.StatusBadRequest, "vehicle_id is required")
		return
	}
	if payload.Event == "" {
		writeError(w, http.StatusBadRequest, "event is required")
		return
	}
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now().UTC()
	}

	subject := eventSubject(payload.Topic, payload.FleetID, payload.VehicleID, payload.Event)
	body, err := json.Marshal(payload)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "encode payload failed")
		return
	}
	if err := nc.Publish(subject, body); err != nil {
		writeError(w, http.StatusBadGateway, "nats publish failed")
		return
	}

	writeJSON(w, http.StatusAccepted, publishAck{
		Status:  "published",
		Subject: subject,
	})
}

func wsFanoutHandler(nc *nats.Conn, cache *gpsCache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		fleetID := strings.TrimSpace(r.URL.Query().Get("fleet_id"))
		vehicleID := strings.TrimSpace(r.URL.Query().Get("vehicle_id"))
		topic := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("topic")))
		if topic == "" {
			topic = topicGPS
		}
		if fleetID == "" {
			writeError(w, http.StatusBadRequest, "fleet_id is required")
			return
		}

		subjects, err := subjectsForTopic(topic, fleetID, vehicleID)
		if err != nil {
			writeError(w, http.StatusBadRequest, "unsupported topic")
			return
		}

		wsConn, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("websocket upgrade failed: %v", err)
			return
		}
		defer wsConn.Close()

		msgCh := make(chan []byte, 256)
		subs := make([]*nats.Subscription, 0, len(subjects))
		for _, subject := range subjects {
			sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
				select {
				case msgCh <- msg.Data:
				default:
					// Drop if client is too slow.
				}
			})
			if err != nil {
				for _, activeSub := range subs {
					activeSub.Unsubscribe()
				}
				_ = wsConn.WriteJSON(map[string]string{"error": "nats subscribe failed"})
				return
			}
			subs = append(subs, sub)
		}
		defer func() {
			for _, sub := range subs {
				sub.Unsubscribe()
			}
		}()

		log.Printf(
			"ws connected: %s topic=%s subjects=%s",
			r.RemoteAddr,
			topic,
			strings.Join(subjects, ","),
		)

		if topic == topicGPS {
			for _, snapshot := range cache.Snapshot(fleetID, vehicleID) {
				_ = wsConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if err := wsConn.WriteMessage(websocket.TextMessage, snapshot); err != nil {
					return
				}
			}
		}

		_ = wsConn.SetReadDeadline(time.Now().Add(120 * time.Second))
		wsConn.SetPongHandler(func(string) error {
			_ = wsConn.SetReadDeadline(time.Now().Add(120 * time.Second))
			return nil
		})

		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				if _, _, err := wsConn.ReadMessage(); err != nil {
					return
				}
			}
		}()

		ticker := time.NewTicker(40 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				log.Printf("ws disconnected: %s", r.RemoteAddr)
				return
			case body := <-msgCh:
				_ = wsConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if err := wsConn.WriteMessage(websocket.TextMessage, body); err != nil {
					return
				}
			case <-ticker.C:
				_ = wsConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if err := wsConn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}
	}
}

func gpsRawSubject(fleetID, vehicleID string) string {
	return fmt.Sprintf("fleet.%s.vehicle.%s.gps.raw", fleetID, vehicleID)
}

func gpsLiveSubject(fleetID, vehicleID string) string {
	return fmt.Sprintf("fleet.%s.vehicle.%s.gps.live", fleetID, vehicleID)
}

func gpsWildcardSubject(fleetID, vehicleID string) string {
	if vehicleID == "" {
		return fmt.Sprintf("fleet.%s.vehicle.*.gps.live", fleetID)
	}
	return gpsLiveSubject(fleetID, vehicleID)
}

func tripSubject(fleetID, vehicleID, event string) string {
	return fmt.Sprintf("fleet.%s.vehicle.%s.trip.%s", fleetID, vehicleID, event)
}

func tripWildcardSubject(fleetID, vehicleID string) string {
	if vehicleID == "" {
		return fmt.Sprintf("fleet.%s.vehicle.*.trip.*", fleetID)
	}
	return fmt.Sprintf("fleet.%s.vehicle.%s.trip.*", fleetID, vehicleID)
}

func opsSubject(fleetID, vehicleID, event string) string {
	if vehicleID == "" {
		return fmt.Sprintf("fleet.%s.ops.%s", fleetID, event)
	}
	return fmt.Sprintf("fleet.%s.vehicle.%s.ops.%s", fleetID, vehicleID, event)
}

func opsWildcardSubjects(fleetID, vehicleID string) []string {
	if vehicleID == "" {
		return []string{
			fmt.Sprintf("fleet.%s.vehicle.*.ops.*", fleetID),
			fmt.Sprintf("fleet.%s.ops.*", fleetID),
		}
	}
	return []string{
		fmt.Sprintf("fleet.%s.vehicle.%s.ops.*", fleetID, vehicleID),
	}
}

func normalizeRealtimeTopic(requestedTopic, event string) string {
	switch requestedTopic {
	case topicTrip, topicOps:
		return requestedTopic
	}

	switch strings.ToLower(strings.TrimSpace(event)) {
	case "trip_started", "trip_completed":
		return topicTrip
	default:
		return topicOps
	}
}

func eventSubject(topic, fleetID, vehicleID, event string) string {
	switch topic {
	case topicTrip:
		return tripSubject(fleetID, vehicleID, event)
	case topicOps:
		return opsSubject(fleetID, vehicleID, event)
	default:
		return tripSubject(fleetID, vehicleID, event)
	}
}

func subjectsForTopic(topic, fleetID, vehicleID string) ([]string, error) {
	switch topic {
	case topicGPS:
		return []string{gpsWildcardSubject(fleetID, vehicleID)}, nil
	case topicTrip:
		return []string{tripWildcardSubject(fleetID, vehicleID)}, nil
	case topicOps:
		return opsWildcardSubjects(fleetID, vehicleID), nil
	case topicEvents:
		subjects := []string{tripWildcardSubject(fleetID, vehicleID)}
		subjects = append(subjects, opsWildcardSubjects(fleetID, vehicleID)...)
		return subjects, nil
	default:
		return nil, fmt.Errorf("unsupported topic")
	}
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s (%s)", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	})
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("write json failed: %v", err)
	}
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func haversineDistance(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371000 // Earth radius in meters
	p1 := lat1 * math.Pi / 180
	p2 := lat2 * math.Pi / 180
	dp := (lat2 - lat1) * math.Pi / 180
	dl := (lon2 - lon1) * math.Pi / 180

	a := math.Sin(dp/2)*math.Sin(dp/2) +
		math.Cos(p1)*math.Cos(p2)*math.Sin(dl/2)*math.Sin(dl/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return R * c
}
