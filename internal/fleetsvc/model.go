// Package fleetsvc manages vehicles, drivers, events, and notices.
// It replaces Firestore collections with NATS KV storage + pub/sub fanout.
package fleetsvc

import (
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Vehicle
// ---------------------------------------------------------------------------

type Vehicle struct {
	ID                 string           `json:"id"`
	RegistrationNumber string           `json:"registration_number"`
	Nickname           string           `json:"nickname,omitempty"`
	Type               string           `json:"type"`
	ServiceType        string           `json:"service_type"`
	IsActive           bool             `json:"is_active"`
	Status             string           `json:"status"`
	StatusMessage      string           `json:"status_message,omitempty"`
	CurrentLocation    *VehicleLocation `json:"current_location,omitempty"`
	CurrentRouteID     string           `json:"current_route_id,omitempty"`
	DriverID           string           `json:"driver_id,omitempty"`
	DriverName         string           `json:"driver_name,omitempty"`
	DriverPhone        string           `json:"driver_phone,omitempty"`
	FleetID            string           `json:"fleet_id"`
	Capacity           int              `json:"capacity,omitempty"`
	CurrentPassengers  int              `json:"current_passengers,omitempty"`
	LastUpdated        time.Time        `json:"last_updated"`
	CreatedAt          time.Time        `json:"created_at"`
}

func (v Vehicle) DiscoveryLabel() string {
	nickname := strings.TrimSpace(v.Nickname)
	if nickname != "" {
		return nickname
	}

	suffix := strings.ToUpper(strings.TrimSpace(v.ID))
	if len(suffix) > 4 {
		suffix = suffix[len(suffix)-4:]
	}
	if suffix == "" {
		return "Vehicle"
	}
	return "Vehicle " + suffix
}

type VehicleLocation struct {
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Heading   float64   `json:"heading,omitempty"`
	Speed     float64   `json:"speed,omitempty"`
	Accuracy  float64   `json:"accuracy,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

type Driver struct {
	ID                 string    `json:"id"`
	Email              string    `json:"email"`
	FullName           string    `json:"full_name"`
	Phone              string    `json:"phone,omitempty"`
	FleetID            string    `json:"fleet_id"`
	AssignedVehicleIDs []string  `json:"assigned_vehicle_ids,omitempty"`
	IsActive           bool      `json:"is_active"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// ---------------------------------------------------------------------------
// Special Event
// ---------------------------------------------------------------------------

type SpecialEvent struct {
	ID           string                 `json:"id"`
	Type         string                 `json:"type"`
	VehicleID    string                 `json:"vehicle_id"`
	DriverID     string                 `json:"driver_id,omitempty"`
	FleetID      string                 `json:"fleet_id"`
	Timestamp    time.Time              `json:"timestamp"`
	Message      string                 `json:"message,omitempty"`
	DelayMinutes int                    `json:"delay_minutes,omitempty"`
	Location     string                 `json:"location,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// ---------------------------------------------------------------------------
// Driver Notice
// ---------------------------------------------------------------------------

type DriverNotice struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Message   string    `json:"message"`
	VehicleID string    `json:"vehicle_id,omitempty"`
	DriverID  string    `json:"driver_id,omitempty"`
	FleetID   string    `json:"fleet_id"`
	RouteID   string    `json:"route_id,omitempty"`
	Priority  string    `json:"priority,omitempty"` // low | medium | high | urgent
	IsRead    bool      `json:"is_read"`
	ActionURL string    `json:"action_url,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	ReadAt    time.Time `json:"read_at,omitempty"`
}
