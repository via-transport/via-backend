package handler

import (
	"context"
	"fmt"
	"testing"

	"via-backend/internal/auth"
	"via-backend/internal/fleetsvc"
)

type realtimeAccessTestStore struct {
	vehicle *fleetsvc.Vehicle
	err     error
}

func (s *realtimeAccessTestStore) GetVehicle(_ context.Context, _, _ string) (*fleetsvc.Vehicle, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.vehicle, nil
}

func TestAuthorizeDriverRealtimePublishAllowsAssignedDriver(t *testing.T) {
	t.Parallel()

	ctx := auth.ContextWithIdentity(context.Background(), auth.Identity{
		UserID: "driver-1",
		Role:   auth.RoleDriver,
	})
	store := &realtimeAccessTestStore{
		vehicle: &fleetsvc.Vehicle{
			ID:       "veh-1",
			FleetID:  "fleet-1",
			DriverID: "driver-1",
		},
	}

	if err := authorizeDriverRealtimePublish(ctx, store, "fleet-1", "veh-1"); err != nil {
		t.Fatalf("expected assigned driver to be allowed, got %v", err)
	}
}

func TestAuthorizeDriverRealtimePublishRejectsDifferentDriver(t *testing.T) {
	t.Parallel()

	ctx := auth.ContextWithIdentity(context.Background(), auth.Identity{
		UserID: "driver-2",
		Role:   auth.RoleDriver,
	})
	store := &realtimeAccessTestStore{
		vehicle: &fleetsvc.Vehicle{
			ID:       "veh-1",
			FleetID:  "fleet-1",
			DriverID: "driver-1",
		},
	}

	if err := authorizeDriverRealtimePublish(ctx, store, "fleet-1", "veh-1"); err == nil {
		t.Fatalf("expected unassigned driver to be rejected")
	}
}

func TestAuthorizeDriverRealtimePublishAllowsServiceIdentity(t *testing.T) {
	t.Parallel()

	ctx := auth.ContextWithIdentity(context.Background(), auth.Identity{
		UserID: "svc-1",
		Role:   auth.RoleService,
	})
	store := &realtimeAccessTestStore{
		err: fmt.Errorf("should not be called"),
	}

	if err := authorizeDriverRealtimePublish(ctx, store, "fleet-1", "veh-1"); err != nil {
		t.Fatalf("expected service identity to bypass vehicle assignment checks, got %v", err)
	}
}
