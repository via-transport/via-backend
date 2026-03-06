package tenantsvc

import (
	"context"
	"testing"
	"time"
)

type memoryStore struct {
	items map[string]*Tenant
}

func newMemoryStore() *memoryStore {
	return &memoryStore{items: make(map[string]*Tenant)}
}

func (s *memoryStore) Put(_ context.Context, tenant *Tenant) error {
	copy := *tenant
	s.items[tenant.ID] = &copy
	return nil
}

func (s *memoryStore) Get(_ context.Context, tenantID string) (*Tenant, error) {
	if item, ok := s.items[tenantID]; ok {
		copy := *item
		return &copy, nil
	}
	return nil, context.DeadlineExceeded
}

func (s *memoryStore) List(_ context.Context) ([]Tenant, error) {
	items := make([]Tenant, 0, len(s.items))
	for _, item := range s.items {
		items = append(items, *item)
	}
	return items, nil
}

func TestEffectiveStatusTransitions(t *testing.T) {
	now := time.Now().UTC()
	tenant := DefaultTrialTenant("tenant-a", "Tenant A", now.Add(-15*24*time.Hour))
	tenant.GraceEndsAt = now.Add(24 * time.Hour)

	if got := tenant.EffectiveStatus(now); got != StatusGrace {
		t.Fatalf("expected %s, got %s", StatusGrace, got)
	}

	tenant.GraceEndsAt = now.Add(-1 * time.Hour)
	if got := tenant.EffectiveStatus(now); got != StatusSuspended {
		t.Fatalf("expected %s, got %s", StatusSuspended, got)
	}
}

func TestGPSRateLimit(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	tenant := DefaultTrialTenant("tenant-b", "Tenant B", time.Now().UTC())
	tenant.SubscriptionStatus = StatusActive
	tenant.PlanType = PlanBasic
	tenant.LocationPublishIntervalS = 10
	if err := store.Put(ctx, tenant); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}

	policy := NewPolicy(store)

	if _, err := policy.CheckGPSPublish(ctx, tenant.ID, "veh-1"); err != nil {
		t.Fatalf("first publish should pass: %v", err)
	}

	if _, err := policy.CheckGPSPublish(ctx, tenant.ID, "veh-1"); err == nil {
		t.Fatal("second publish should be rate-limited")
	} else if pe, ok := AsPolicyError(err); !ok || pe.Code != "location_rate_limited" {
		t.Fatalf("expected location_rate_limited policy error, got: %v", err)
	}
}

func TestCheckVehicleCreateAllowsOperationalTrialCapacity(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	tenant := DefaultTrialTenant("tenant-c", "Tenant C", time.Now().UTC())
	if err := store.Put(ctx, tenant); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}

	policy := NewPolicy(store)

	if _, err := policy.CheckVehicleCreate(ctx, tenant.ID, 2); err != nil {
		t.Fatalf("expected trial tenant with two vehicles to remain within operational capacity: %v", err)
	}
}

func TestPlanSummaryNormalizesLegacyTrialLimits(t *testing.T) {
	now := time.Now().UTC()
	tenant := &Tenant{
		ID:                 "tenant-d",
		Name:               "Tenant D",
		PlanType:           PlanTrial,
		SubscriptionStatus: StatusTrial,
		VehicleLimit:       2,
		PassengerLimit:     100,
		DriverLimit:        2,
		EventHourlyLimit:   30,
	}

	view := tenant.PlanSummary(now)

	if view.VehicleLimit != minOperationalVehicleLimit {
		t.Fatalf("expected vehicle limit %d, got %d", minOperationalVehicleLimit, view.VehicleLimit)
	}
	if view.PassengerLimit != minOperationalPassengerLimit {
		t.Fatalf("expected passenger limit %d, got %d", minOperationalPassengerLimit, view.PassengerLimit)
	}
	if view.DriverLimit != minOperationalDriverLimit {
		t.Fatalf("expected driver limit %d, got %d", minOperationalDriverLimit, view.DriverLimit)
	}
	if view.EventHourlyLimit != minOperationalEventHourlyLimit {
		t.Fatalf("expected event limit %d, got %d", minOperationalEventHourlyLimit, view.EventHourlyLimit)
	}
}
