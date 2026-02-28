package handler

import (
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"

	"via-backend/internal/cache"
	"via-backend/internal/config"
	"via-backend/internal/messaging"
	"via-backend/internal/model"
)

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 8192,
	CheckOrigin:     func(_ *http.Request) bool { return true },
}

// WSFanout handles GET /ws. It upgrades the connection, subscribes to the
// requested NATS subjects, and streams messages to the client.
func WSFanout(broker *messaging.Broker, gpsCache *cache.GPSCache, cfg config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		fleetID := strings.TrimSpace(r.URL.Query().Get("fleet_id"))
		vehicleID := strings.TrimSpace(r.URL.Query().Get("vehicle_id"))
		topic := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("topic")))
		if topic == "" {
			topic = model.TopicGPS
		}
		if fleetID == "" {
			writeError(w, http.StatusBadRequest, "fleet_id is required")
			return
		}

		subjects, err := messaging.SubjectsForTopic(topic, fleetID, vehicleID)
		if err != nil {
			writeError(w, http.StatusBadRequest, "unsupported topic")
			return
		}

		wsConn, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("[ws] upgrade failed: %v", err)
			return
		}
		defer wsConn.Close()

		// ---- NATS subscriptions ----
		msgCh := make(chan []byte, cfg.WSChannelBuffer)
		subs := make([]*nats.Subscription, 0, len(subjects))
		for _, subj := range subjects {
			sub, err := broker.Subscribe(subj, msgCh)
			if err != nil {
				unsubAll(subs)
				_ = wsConn.WriteJSON(map[string]string{"error": "subscribe failed"})
				return
			}
			subs = append(subs, sub)
		}
		defer unsubAll(subs)

		log.Printf("[ws] connected: %s topic=%s subjects=%s",
			r.RemoteAddr, topic, strings.Join(subjects, ","))

		// ---- Bootstrap: send cached positions ----
		if topic == model.TopicGPS {
			for _, snap := range gpsCache.Snapshot(fleetID, vehicleID) {
				_ = wsConn.SetWriteDeadline(time.Now().Add(cfg.WSWriteTimeout))
				if err := wsConn.WriteMessage(websocket.TextMessage, snap); err != nil {
					return
				}
			}
		}

		// ---- Read pump (detect disconnects + pong) ----
		_ = wsConn.SetReadDeadline(time.Now().Add(cfg.WSReadTimeout))
		wsConn.SetPongHandler(func(string) error {
			_ = wsConn.SetReadDeadline(time.Now().Add(cfg.WSReadTimeout))
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

		// ---- Write pump ----
		ticker := time.NewTicker(cfg.WSPingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				log.Printf("[ws] disconnected: %s", r.RemoteAddr)
				return
			case body := <-msgCh:
				_ = wsConn.SetWriteDeadline(time.Now().Add(cfg.WSWriteTimeout))
				if err := wsConn.WriteMessage(websocket.TextMessage, body); err != nil {
					return
				}
			case <-ticker.C:
				_ = wsConn.SetWriteDeadline(time.Now().Add(cfg.WSWriteTimeout))
				if err := wsConn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}
	}
}

func unsubAll(subs []*nats.Subscription) {
	for _, s := range subs {
		_ = s.Unsubscribe()
	}
}
