package tenantsvc

import (
	"strings"
	"time"
)

const (
	PlanTrial      = "TRIAL"
	PlanBasic      = "BASIC"
	PlanPro        = "PRO"
	PlanEnterprise = "ENTERPRISE"

	StatusTrial     = "TRIAL"
	StatusActive    = "ACTIVE"
	StatusGrace     = "GRACE"
	StatusSuspended = "SUSPENDED"

	minOperationalVehicleLimit     = 10
	minOperationalPassengerLimit   = 250
	minOperationalDriverLimit      = 10
	minLocationPublishIntervalS    = 3
	minOperationalEventHourlyLimit = 60
)

// Tenant is the billing and authorization boundary. In the current codebase
// the existing fleet_id is treated as the tenant identifier.
type Tenant struct {
	ID                       string    `json:"id"`
	Name                     string    `json:"name"`
	PlanType                 string    `json:"plan_type"`
	SubscriptionStatus       string    `json:"subscription_status"`
	TrialStartedAt           time.Time `json:"trial_started_at,omitempty"`
	TrialEndsAt              time.Time `json:"trial_ends_at,omitempty"`
	GraceEndsAt              time.Time `json:"grace_ends_at,omitempty"`
	VehicleLimit             int       `json:"vehicle_limit"`
	PassengerLimit           int       `json:"passenger_limit"`
	DriverLimit              int       `json:"driver_limit"`
	LocationPublishIntervalS int       `json:"location_publish_interval_seconds"`
	EventHourlyLimit         int       `json:"event_hourly_limit"`
	CreatedAt                time.Time `json:"created_at"`
	UpdatedAt                time.Time `json:"updated_at"`
}

type CreateTenantRequest struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type StartTrialRequest struct {
	FleetID string `json:"fleet_id"`
	Name    string `json:"name,omitempty"`
}

type UpdateBillingStatusRequest struct {
	FleetID            string `json:"fleet_id"`
	PlanType           string `json:"plan_type,omitempty"`
	SubscriptionStatus string `json:"subscription_status,omitempty"`
}

type PlanView struct {
	TenantID                 string    `json:"tenant_id"`
	Name                     string    `json:"name"`
	PlanType                 string    `json:"plan_type"`
	SubscriptionStatus       string    `json:"subscription_status"`
	EffectiveStatus          string    `json:"effective_status"`
	RealtimeEnabled          bool      `json:"realtime_enabled"`
	VehicleLimit             int       `json:"vehicle_limit"`
	PassengerLimit           int       `json:"passenger_limit"`
	DriverLimit              int       `json:"driver_limit"`
	LocationPublishIntervalS int       `json:"location_publish_interval_seconds"`
	EventHourlyLimit         int       `json:"event_hourly_limit"`
	TrialStartedAt           time.Time `json:"trial_started_at,omitempty"`
	TrialEndsAt              time.Time `json:"trial_ends_at,omitempty"`
	GraceEndsAt              time.Time `json:"grace_ends_at,omitempty"`
	OwnerMessage             string    `json:"owner_message"`
	PublicMessage            string    `json:"public_message"`
}

func DefaultTrialTenant(tenantID, name string, now time.Time) *Tenant {
	if name == "" {
		name = tenantID
	}
	return &Tenant{
		ID:                       tenantID,
		Name:                     name,
		PlanType:                 PlanTrial,
		SubscriptionStatus:       StatusTrial,
		TrialStartedAt:           now,
		TrialEndsAt:              now.Add(14 * 24 * time.Hour),
		GraceEndsAt:              now.Add((14 + 7) * 24 * time.Hour),
		VehicleLimit:             minOperationalVehicleLimit,
		PassengerLimit:           minOperationalPassengerLimit,
		DriverLimit:              minOperationalDriverLimit,
		LocationPublishIntervalS: minLocationPublishIntervalS,
		EventHourlyLimit:         minOperationalEventHourlyLimit,
		CreatedAt:                now,
		UpdatedAt:                now,
	}
}

func (t *Tenant) EffectiveStatus(now time.Time) string {
	status := upperOrDefault(t.SubscriptionStatus, StatusTrial)

	switch status {
	case StatusSuspended:
		return StatusSuspended
	case StatusActive:
		return StatusActive
	case StatusGrace:
		if !t.GraceEndsAt.IsZero() && now.After(t.GraceEndsAt) {
			return StatusSuspended
		}
		return StatusGrace
	case StatusTrial:
		if !t.TrialEndsAt.IsZero() && now.After(t.TrialEndsAt) {
			if !t.GraceEndsAt.IsZero() && now.After(t.GraceEndsAt) {
				return StatusSuspended
			}
			return StatusGrace
		}
		return StatusTrial
	default:
		return StatusTrial
	}
}

func (t *Tenant) PlanSummary(now time.Time) PlanView {
	effective := t.EffectiveStatus(now)
	view := PlanView{
		TenantID:                 t.ID,
		Name:                     t.Name,
		PlanType:                 upperOrDefault(t.PlanType, PlanTrial),
		SubscriptionStatus:       upperOrDefault(t.SubscriptionStatus, StatusTrial),
		EffectiveStatus:          effective,
		RealtimeEnabled:          effective != StatusSuspended,
		VehicleLimit:             max(t.VehicleLimit, minOperationalVehicleLimit),
		PassengerLimit:           max(t.PassengerLimit, minOperationalPassengerLimit),
		DriverLimit:              max(t.DriverLimit, minOperationalDriverLimit),
		LocationPublishIntervalS: max(t.LocationPublishIntervalS, minLocationPublishIntervalS),
		EventHourlyLimit:         max(t.EventHourlyLimit, minOperationalEventHourlyLimit),
		TrialStartedAt:           t.TrialStartedAt,
		TrialEndsAt:              t.TrialEndsAt,
		GraceEndsAt:              t.GraceEndsAt,
		PublicMessage:            "Service is available.",
	}

	switch effective {
	case StatusTrial:
		view.OwnerMessage = "Trial is active."
	case StatusGrace:
		view.OwnerMessage = "Trial expired. Grace period is active."
	case StatusActive:
		view.OwnerMessage = "Subscription is active."
	case StatusSuspended:
		view.OwnerMessage = "Subscription is suspended. Realtime features are blocked."
		view.PublicMessage = "Tracking temporarily unavailable."
	}

	return view
}

func upperOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return strings.ToUpper(strings.TrimSpace(value))
}

func max(value, fallback int) int {
	if value < fallback {
		return fallback
	}
	return value
}
