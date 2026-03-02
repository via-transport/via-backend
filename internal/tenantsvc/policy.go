package tenantsvc

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Policy struct {
	store Store

	mu              sync.Mutex
	lastGPSByKey    map[string]time.Time
	eventTimesByKey map[string][]time.Time
}

func NewPolicy(store Store) *Policy {
	return &Policy{
		store:           store,
		lastGPSByKey:    make(map[string]time.Time),
		eventTimesByKey: make(map[string][]time.Time),
	}
}

type PolicyError struct {
	HTTPStatus    int    `json:"-"`
	Code          string `json:"code"`
	Message       string `json:"error"`
	PublicMessage string `json:"public_message,omitempty"`
}

func (e *PolicyError) Error() string { return e.Message }

func AsPolicyError(err error) (*PolicyError, bool) {
	pe, ok := err.(*PolicyError)
	return pe, ok
}

func (p *Policy) EnsureRealtimeAllowed(ctx context.Context, tenantID string) (*PlanView, error) {
	tenant, err := p.ensureTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	view := tenant.PlanSummary(nowUTC())
	if !view.RealtimeEnabled {
		return nil, &PolicyError{
			HTTPStatus:    http.StatusPaymentRequired,
			Code:          "tenant_suspended",
			Message:       "service temporarily unavailable",
			PublicMessage: view.PublicMessage,
		}
	}
	return &view, nil
}

func (p *Policy) CheckGPSPublish(ctx context.Context, tenantID, vehicleID string) (*PlanView, error) {
	view, err := p.EnsureRealtimeAllowed(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	key := tenantID + ":" + vehicleID
	now := nowUTC()

	p.mu.Lock()
	defer p.mu.Unlock()

	interval := time.Duration(view.LocationPublishIntervalS) * time.Second
	if interval <= 0 {
		interval = 3 * time.Second
	}
	if last, ok := p.lastGPSByKey[key]; ok && now.Sub(last) < interval {
		return nil, &PolicyError{
			HTTPStatus: http.StatusTooManyRequests,
			Code:       "location_rate_limited",
			Message:    fmt.Sprintf("location publish rate limit exceeded: one update every %ds", view.LocationPublishIntervalS),
		}
	}
	p.lastGPSByKey[key] = now
	return view, nil
}

func (p *Policy) CheckEventPublish(ctx context.Context, tenantID, driverID string) (*PlanView, error) {
	view, err := p.EnsureRealtimeAllowed(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	key := tenantID + ":" + driverID
	now := nowUTC()
	cutoff := now.Add(-1 * time.Hour)

	p.mu.Lock()
	defer p.mu.Unlock()

	hits := p.eventTimesByKey[key]
	filtered := hits[:0]
	for _, hit := range hits {
		if hit.After(cutoff) {
			filtered = append(filtered, hit)
		}
	}
	if len(filtered) >= max(view.EventHourlyLimit, 30) {
		p.eventTimesByKey[key] = filtered
		return nil, &PolicyError{
			HTTPStatus: http.StatusTooManyRequests,
			Code:       "event_rate_limited",
			Message:    "event publish rate limit exceeded",
		}
	}
	filtered = append(filtered, now)
	p.eventTimesByKey[key] = filtered
	return view, nil
}

func (p *Policy) CheckVehicleCreate(ctx context.Context, tenantID string, currentCount int) (*PlanView, error) {
	tenant, err := p.ensureTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	view := tenant.PlanSummary(nowUTC())
	if view.EffectiveStatus == StatusTrial && currentCount >= view.VehicleLimit {
		return nil, &PolicyError{
			HTTPStatus: http.StatusForbidden,
			Code:       "vehicle_limit_reached",
			Message:    "vehicle limit reached for current tenant plan",
		}
	}
	if view.EffectiveStatus == StatusSuspended {
		return nil, &PolicyError{
			HTTPStatus:    http.StatusPaymentRequired,
			Code:          "tenant_suspended",
			Message:       "service temporarily unavailable",
			PublicMessage: view.PublicMessage,
		}
	}
	return &view, nil
}

func (p *Policy) CheckDriverCreate(ctx context.Context, tenantID string, currentCount int) (*PlanView, error) {
	tenant, err := p.ensureTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	view := tenant.PlanSummary(nowUTC())
	if view.EffectiveStatus == StatusTrial && currentCount >= view.DriverLimit {
		return nil, &PolicyError{
			HTTPStatus: http.StatusForbidden,
			Code:       "driver_limit_reached",
			Message:    "driver limit reached for current tenant plan",
		}
	}
	return &view, nil
}

func (p *Policy) CheckPassengerActivate(ctx context.Context, tenantID string, currentCount int) (*PlanView, error) {
	tenant, err := p.ensureTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	view := tenant.PlanSummary(nowUTC())
	if view.EffectiveStatus == StatusTrial && currentCount >= view.PassengerLimit {
		return nil, &PolicyError{
			HTTPStatus: http.StatusForbidden,
			Code:       "passenger_limit_reached",
			Message:    "passenger limit reached for current tenant plan",
		}
	}
	if view.EffectiveStatus == StatusSuspended {
		return nil, &PolicyError{
			HTTPStatus:    http.StatusPaymentRequired,
			Code:          "tenant_suspended",
			Message:       "service temporarily unavailable",
			PublicMessage: view.PublicMessage,
		}
	}
	return &view, nil
}

func (p *Policy) GetOrCreatePlan(ctx context.Context, tenantID string) (*PlanView, error) {
	tenant, err := p.ensureTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	view := tenant.PlanSummary(nowUTC())
	return &view, nil
}

func (p *Policy) UpsertTenant(ctx context.Context, tenant *Tenant) (*PlanView, error) {
	if tenant.ID == "" {
		return nil, fmt.Errorf("tenant id is required")
	}
	now := nowUTC()
	if tenant.CreatedAt.IsZero() {
		tenant.CreatedAt = now
	}
	tenant.UpdatedAt = now
	if err := p.store.Put(ctx, tenant); err != nil {
		return nil, err
	}
	view := tenant.PlanSummary(now)
	return &view, nil
}

func (p *Policy) ensureTenant(ctx context.Context, tenantID string) (*Tenant, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("fleet_id is required")
	}
	tenant, err := p.store.Get(ctx, tenantID)
	if err == nil {
		return tenant, nil
	}

	seed := DefaultTrialTenant(tenantID, tenantID, nowUTC())
	if putErr := p.store.Put(ctx, seed); putErr != nil {
		return nil, putErr
	}
	return seed, nil
}

func nowUTC() time.Time {
	return time.Now().UTC()
}
