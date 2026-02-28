package model

import "time"

// RealtimeEvent represents an operational or trip event.
type RealtimeEvent struct {
	FleetID   string    `json:"fleet_id"`
	VehicleID string    `json:"vehicle_id"`
	DriverID  string    `json:"driver_id,omitempty"`
	RouteID   string    `json:"route_id,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	Message   string    `json:"message,omitempty"`
	Event     string    `json:"event,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// PublishAck is the standard response after a successful NATS publish.
type PublishAck struct {
	Status  string `json:"status"`
	Subject string `json:"subject"`
}
