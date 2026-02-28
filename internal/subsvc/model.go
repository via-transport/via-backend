package subsvc

import "time"

// Subscription represents a passenger subscribing to track a vehicle.
type Subscription struct {
	ID          string    `json:"id"`
	UserID      string    `json:"user_id"`
	VehicleID   string    `json:"vehicle_id"`
	FleetID     string    `json:"fleet_id"`
	Status      string    `json:"status"` // "active", "paused", "cancelled"
	Preferences SubPrefs  `json:"preferences"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
}

// SubPrefs holds notification preferences for a subscription.
type SubPrefs struct {
	NotifyOnArrival bool `json:"notify_on_arrival"`
	NotifyOnDelay   bool `json:"notify_on_delay"`
	NotifyOnEvent   bool `json:"notify_on_event"`
	RadiusMeters    int  `json:"radius_meters,omitempty"` // proximity alert radius
}
