package handler

import (
	"context"
	"fmt"
	"strings"

	"via-backend/internal/auth"
	"via-backend/internal/fleetsvc"
)

type driverAssignmentReader interface {
	GetVehicle(ctx context.Context, fleetID, vehicleID string) (*fleetsvc.Vehicle, error)
}

func authorizeDriverRealtimePublish(
	ctx context.Context,
	assignments driverAssignmentReader,
	fleetID string,
	vehicleID string,
) error {
	id := auth.IdentityFromContext(ctx)
	if id.Role == "" || id.Role == auth.RoleService {
		return nil
	}
	if id.Role != auth.RoleDriver {
		return nil
	}
	if assignments == nil {
		return fmt.Errorf("vehicle assignment store unavailable")
	}

	normalizedFleetID := strings.TrimSpace(fleetID)
	normalizedVehicleID := strings.TrimSpace(vehicleID)
	normalizedDriverID := strings.TrimSpace(id.UserID)
	if normalizedFleetID == "" || normalizedVehicleID == "" {
		return fmt.Errorf("fleet_id and vehicle_id are required")
	}
	if normalizedDriverID == "" {
		return fmt.Errorf("driver authentication required")
	}

	vehicle, err := assignments.GetVehicle(ctx, normalizedFleetID, normalizedVehicleID)
	if err != nil || vehicle == nil {
		return fmt.Errorf("vehicle assignment not found")
	}
	if strings.TrimSpace(vehicle.DriverID) != normalizedDriverID {
		return fmt.Errorf("driver is not assigned to this vehicle")
	}
	return nil
}
