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

// JoinRequest is the owner-facing passenger access request projection.
// It keeps the subscription contract while enriching it with passenger profile
// fields needed for review.
type JoinRequest struct {
	ID          string    `json:"id"`
	UserID      string    `json:"user_id"`
	VehicleID   string    `json:"vehicle_id"`
	FleetID     string    `json:"fleet_id"`
	Status      string    `json:"status"`
	Preferences SubPrefs  `json:"preferences"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`

	PassengerName           string     `json:"passenger_name,omitempty"`
	PassengerEmail          string     `json:"passenger_email,omitempty"`
	PassengerPhone          string     `json:"passenger_phone,omitempty"`
	PassengerWorkplace      string     `json:"passenger_workplace,omitempty"`
	PassengerAddress        string     `json:"passenger_address,omitempty"`
	PassengerEmployeeNumber string     `json:"passenger_employee_number,omitempty"`
	PassengerJoinedAt       *time.Time `json:"passenger_joined_at,omitempty"`
}

// SubPrefs holds notification preferences for a subscription.
type SubPrefs struct {
	NotifyOnArrival bool `json:"notify_on_arrival"`
	NotifyOnDelay   bool `json:"notify_on_delay"`
	NotifyOnEvent   bool `json:"notify_on_event"`
	RadiusMeters    int  `json:"radius_meters,omitempty"` // proximity alert radius
}
