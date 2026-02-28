// Package notifysvc – event_pipeline bridges fleet events to per-user
// notifications.  When a driver creates a SpecialEvent (e.g. trip_started),
// the fleet service publishes a NATS message on
// "fleet.<fleetID>.vehicle.<vehicleID>.ops.<event>".  This pipeline
// subscribes to those subjects, looks up all active vehicle subscribers,
// and creates a Notification record for each — persisting it and pushing
// it over WebSocket in real-time.
package notifysvc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"via-backend/internal/messaging"
	"via-backend/internal/subsvc"
)

// fleetEvent is the JSON payload published by fleetsvc.fanoutVehicleChange.
type fleetEvent struct {
	FleetID   string `json:"fleet_id"`
	VehicleID string `json:"vehicle_id"`
	Event     string `json:"event"`     // e.g. "event_trip_started"
	Timestamp string `json:"timestamp"` // RFC3339
	DriverID  string `json:"driver_id,omitempty"`
	Message   string `json:"message,omitempty"`
}

// legacyRealtimeEvent is the JSON payload published by the legacy
// /v1/events/publish endpoint.
type legacyRealtimeEvent struct {
	FleetID   string `json:"fleet_id"`
	VehicleID string `json:"vehicle_id"`
	DriverID  string `json:"driver_id,omitempty"`
	Topic     string `json:"topic,omitempty"`
	Event     string `json:"event,omitempty"`
	Message   string `json:"message,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
}

// SubscribeFleetEvents starts a NATS subscriber that listens for all fleet
// vehicle operations and automatically creates notifications for each
// subscriber of the affected vehicle.
//
// It subscribes to:
//   - fleet.*.vehicle.*.ops.>   (new microservice events from fleetsvc)
//   - fleet.*.events.>          (legacy realtime events from /v1/events/publish)
func (h *Handler) SubscribeFleetEvents(broker *messaging.Broker, subStore subsvc.SubStore) {
	if broker.NC == nil {
		log.Printf("[event-pipeline] no NATS connection, event→notification pipeline disabled")
		return
	}
	nc := broker.NC

	// Subscribe to new microservice events: fleet.<fleet>.vehicle.<vehicle>.ops.<event>
	_, err := nc.Subscribe("fleet.*.vehicle.*.ops.>", func(msg *nats.Msg) {
		var ev fleetEvent
		if err := json.Unmarshal(msg.Data, &ev); err != nil {
			log.Printf("[event-pipeline] unmarshal fleet event: %v", err)
			return
		}
		if ev.VehicleID == "" {
			return
		}
		h.createNotificationsForEvent(subStore, ev.FleetID, ev.VehicleID, ev.Event, ev.Message, ev.Timestamp)
	})
	if err != nil {
		log.Printf("[event-pipeline] NATS subscribe fleet.*.vehicle.*.ops.>: %v", err)
	} else {
		log.Printf("[event-pipeline] subscribed to fleet.*.vehicle.*.ops.>")
	}

	// Subscribe to legacy realtime events: fleet.<fleet>.events.<topic>
	_, err = nc.Subscribe("fleet.*.events.>", func(msg *nats.Msg) {
		var ev legacyRealtimeEvent
		if err := json.Unmarshal(msg.Data, &ev); err != nil {
			log.Printf("[event-pipeline] unmarshal legacy event: %v", err)
			return
		}
		if ev.VehicleID == "" {
			return
		}
		eventType := ev.Event
		if eventType == "" {
			eventType = ev.Topic
		}
		h.createNotificationsForEvent(subStore, ev.FleetID, ev.VehicleID, eventType, ev.Message, ev.Timestamp)
	})
	if err != nil {
		log.Printf("[event-pipeline] NATS subscribe fleet.*.events.>: %v", err)
	} else {
		log.Printf("[event-pipeline] subscribed to fleet.*.events.>")
	}
}

// createNotificationsForEvent looks up all subscribers for a vehicle and
// creates a notification for each one.
func (h *Handler) createNotificationsForEvent(
	subStore subsvc.SubStore,
	fleetID, vehicleID, eventType, message, timestamp string,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Look up all active subscriptions for this vehicle.
	subs, err := subStore.ListForVehicle(ctx, vehicleID)
	if err != nil {
		log.Printf("[event-pipeline] list subscribers for %s: %v", vehicleID, err)
		return
	}

	// Build human-readable title and body from event type.
	title, body := eventToNotificationText(eventType, vehicleID, message)

	// Track unique user IDs to avoid duplicate notifications.
	notifiedUsers := make(map[string]bool)

	for _, sub := range subs {
		// Only notify active subscribers who opted into event notifications.
		if sub.Status != "active" {
			continue
		}
		if !shouldNotifyForEvent(sub.Preferences, eventType) {
			continue
		}
		if notifiedUsers[sub.UserID] {
			continue
		}
		notifiedUsers[sub.UserID] = true

		n := &Notification{
			ID:        uuid.New().String(),
			UserID:    sub.UserID,
			FleetID:   fleetID,
			VehicleID: vehicleID,
			Type:      "event",
			Title:     title,
			Body:      body,
			Data: map[string]string{
				"event_type": eventType,
				"vehicle_id": vehicleID,
			},
			CreatedAt: time.Now().UTC(),
			IsRead:    false,
		}

		if err := h.store.Put(ctx, n); err != nil {
			log.Printf("[event-pipeline] store notification for user %s: %v", sub.UserID, err)
			continue
		}

		// Push via WebSocket hub.
		unread, _ := h.store.CountUnread(ctx, sub.UserID)
		payload := NotificationPayload{
			Action:       "new",
			Notification: n,
			UnreadCount:  unread,
		}
		data, _ := json.Marshal(payload)
		h.hub.SendToUser(sub.UserID, data)

		// Publish to NATS for cross-instance delivery.
		subject := "notify." + sub.UserID
		if err := h.broker.Publish(subject, data); err != nil {
			log.Printf("[event-pipeline] NATS publish %s: %v", subject, err)
		}
	}

	// Also create a fleet-admin notification so the admin/owner app can see it.
	adminUserID := "admin-" + fleetID
	if !notifiedUsers[adminUserID] {
		n := &Notification{
			ID:        uuid.New().String(),
			UserID:    adminUserID,
			FleetID:   fleetID,
			VehicleID: vehicleID,
			Type:      "event",
			Title:     title,
			Body:      body,
			Data: map[string]string{
				"event_type": eventType,
				"vehicle_id": vehicleID,
				"for_admin":  "true",
			},
			CreatedAt: time.Now().UTC(),
			IsRead:    false,
		}
		if err := h.store.Put(ctx, n); err != nil {
			log.Printf("[event-pipeline] store admin notification: %v", err)
		} else {
			unread, _ := h.store.CountUnread(ctx, adminUserID)
			payload := NotificationPayload{
				Action:       "new",
				Notification: n,
				UnreadCount:  unread,
			}
			data, _ := json.Marshal(payload)
			h.hub.SendToUser(adminUserID, data)
			_ = h.broker.Publish("notify."+adminUserID, data)
		}
	}

	if len(notifiedUsers) > 0 {
		log.Printf("[event-pipeline] event %s on vehicle %s → notified %d user(s)",
			eventType, vehicleID, len(notifiedUsers))
	}
}

// shouldNotifyForEvent checks subscription preferences to decide whether to
// send a notification for the given event type.
func shouldNotifyForEvent(prefs subsvc.SubPrefs, eventType string) bool {
	lower := strings.ToLower(eventType)
	// Delay events
	if strings.Contains(lower, "delay") {
		return prefs.NotifyOnDelay
	}
	// Arrival events
	if strings.Contains(lower, "arrival") || strings.Contains(lower, "arriving") {
		return prefs.NotifyOnArrival
	}
	// All other events (trip_started, breakdown, etc.)
	return prefs.NotifyOnEvent
}

// eventToNotificationText converts a raw event type string into a human-readable
// title and body for the notification.
func eventToNotificationText(eventType, vehicleID, message string) (title, body string) {
	// Strip "event_" prefix from fanout subject naming.
	clean := strings.TrimPrefix(eventType, "event_")
	clean = strings.ToLower(clean)

	switch clean {
	case "trip_started":
		title = "Trip Started"
		body = fmt.Sprintf("Vehicle %s has started its trip.", vehicleID)
	case "trip_completed":
		title = "Trip Completed"
		body = fmt.Sprintf("Vehicle %s has completed its trip.", vehicleID)
	case "breakdown":
		title = "Vehicle Breakdown"
		body = fmt.Sprintf("Vehicle %s has reported a breakdown.", vehicleID)
	case "delay_minor", "delay_major":
		severity := "minor"
		if clean == "delay_major" {
			severity = "major"
		}
		title = "Vehicle Delayed"
		body = fmt.Sprintf("Vehicle %s is experiencing a %s delay.", vehicleID, severity)
	case "route_updated":
		title = "Route Updated"
		body = fmt.Sprintf("Vehicle %s has updated its route.", vehicleID)
	case "emergency_stop":
		title = "Emergency Stop"
		body = fmt.Sprintf("Vehicle %s has made an emergency stop.", vehicleID)
	case "tea_break":
		title = "Driver Break"
		body = fmt.Sprintf("Vehicle %s driver is taking a break.", vehicleID)
	case "passenger_pickup":
		title = "Passenger Pickup"
		body = fmt.Sprintf("Vehicle %s is picking up passengers.", vehicleID)
	case "passenger_dropoff":
		title = "Passenger Dropoff"
		body = fmt.Sprintf("Vehicle %s is dropping off passengers.", vehicleID)
	default:
		title = "Vehicle Event"
		body = fmt.Sprintf("Vehicle %s: %s", vehicleID, clean)
	}

	// Append custom message if provided.
	if message != "" {
		body += " — " + message
	}
	return
}
