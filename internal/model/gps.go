// Package model defines the domain types shared across layers.
package model

import "time"

// GPSUpdate is a single GPS telemetry point from a driver device.
type GPSUpdate struct {
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
