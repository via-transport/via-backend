package fleetsvc

import (
	"context"
	"time"
)

const (
	demoVehicleID         = "veh_001"
	demoDriverID          = "drv_001"
	demoVehicleRegNo      = "ND-4521"
	demoVehicleType       = "bus"
	demoVehicleService    = "school"
	demoVehicleStatus     = "en_route"
	demoVehicleStatusNote = "Running on schedule"
	demoRouteID           = "route_010"
	demoDriverName        = "Kamal Perera"
	demoDriverEmail       = "kamal.perera@via.local"
	demoDriverPhone       = "+94770001122"
)

// EnsureDemoFleet provisions a stable demo driver/vehicle pair in the backend
// so all apps can point at the same shared records instead of local mocks.
func EnsureDemoFleet(ctx context.Context, store FleetStore, fleetID string) error {
	if fleetID == "" {
		return nil
	}

	now := time.Now().UTC()

	driver, err := ensureDemoDriver(ctx, store, fleetID, now)
	if err != nil {
		return err
	}

	if err := ensureDemoVehicle(ctx, store, fleetID, now, driver); err != nil {
		return err
	}

	return nil
}

func ensureDemoDriver(
	ctx context.Context,
	store FleetStore,
	fleetID string,
	now time.Time,
) (*Driver, error) {
	driver, err := store.GetDriver(ctx, fleetID, demoDriverID)
	if err != nil {
		driver = &Driver{
			ID:                 demoDriverID,
			Email:              demoDriverEmail,
			FullName:           demoDriverName,
			Phone:              demoDriverPhone,
			FleetID:            fleetID,
			AssignedVehicleIDs: []string{demoVehicleID},
			IsActive:           true,
			CreatedAt:          now,
			UpdatedAt:          now,
		}
		if err := store.PutDriver(ctx, driver); err != nil {
			return nil, err
		}
		return driver, nil
	}

	changed := false
	if driver.Email == "" {
		driver.Email = demoDriverEmail
		changed = true
	}
	if driver.FullName == "" {
		driver.FullName = demoDriverName
		changed = true
	}
	if driver.Phone == "" {
		driver.Phone = demoDriverPhone
		changed = true
	}
	if !driver.IsActive {
		driver.IsActive = true
		changed = true
	}
	if !containsDemoID(driver.AssignedVehicleIDs, demoVehicleID) {
		driver.AssignedVehicleIDs = append(driver.AssignedVehicleIDs, demoVehicleID)
		changed = true
	}

	if changed {
		if driver.CreatedAt.IsZero() {
			driver.CreatedAt = now
		}
		driver.UpdatedAt = now
		if err := store.PutDriver(ctx, driver); err != nil {
			return nil, err
		}
	}

	return driver, nil
}

func ensureDemoVehicle(
	ctx context.Context,
	store FleetStore,
	fleetID string,
	now time.Time,
	driver *Driver,
) error {
	vehicle, err := store.GetVehicle(ctx, fleetID, demoVehicleID)
	if err != nil {
		vehicle = &Vehicle{
			ID:                 demoVehicleID,
			RegistrationNumber: demoVehicleRegNo,
			Type:               demoVehicleType,
			ServiceType:        demoVehicleService,
			IsActive:           true,
			Status:             demoVehicleStatus,
			StatusMessage:      demoVehicleStatusNote,
			CurrentRouteID:     demoRouteID,
			DriverID:           driver.ID,
			DriverName:         driver.FullName,
			DriverPhone:        driver.Phone,
			FleetID:            fleetID,
			Capacity:           45,
			CurrentLocation: &VehicleLocation{
				Latitude:  6.9271,
				Longitude: 79.8612,
				Heading:   90,
				Speed:     38,
				Timestamp: now,
			},
			LastUpdated: now,
			CreatedAt:   now,
		}
		return store.PutVehicle(ctx, vehicle)
	}

	changed := false
	if vehicle.RegistrationNumber == "" {
		vehicle.RegistrationNumber = demoVehicleRegNo
		changed = true
	}
	if vehicle.Type == "" {
		vehicle.Type = demoVehicleType
		changed = true
	}
	if vehicle.ServiceType == "" {
		vehicle.ServiceType = demoVehicleService
		changed = true
	}
	if vehicle.Status == "" {
		vehicle.Status = demoVehicleStatus
		changed = true
	}
	if vehicle.StatusMessage == "" {
		vehicle.StatusMessage = demoVehicleStatusNote
		changed = true
	}
	if vehicle.CurrentRouteID == "" {
		vehicle.CurrentRouteID = demoRouteID
		changed = true
	}
	if !vehicle.IsActive {
		vehicle.IsActive = true
		changed = true
	}
	if vehicle.Capacity <= 0 {
		vehicle.Capacity = 45
		changed = true
	}
	if vehicle.CurrentLocation == nil {
		vehicle.CurrentLocation = &VehicleLocation{
			Latitude:  6.9271,
			Longitude: 79.8612,
			Heading:   90,
			Speed:     38,
			Timestamp: now,
		}
		changed = true
	}
	if vehicle.DriverID == "" {
		vehicle.DriverID = driver.ID
		vehicle.DriverName = driver.FullName
		vehicle.DriverPhone = driver.Phone
		changed = true
	} else if vehicle.DriverID == driver.ID {
		if vehicle.DriverName != driver.FullName {
			vehicle.DriverName = driver.FullName
			changed = true
		}
		if vehicle.DriverPhone != driver.Phone {
			vehicle.DriverPhone = driver.Phone
			changed = true
		}
	}

	if changed {
		if vehicle.CreatedAt.IsZero() {
			vehicle.CreatedAt = now
		}
		vehicle.LastUpdated = now
		return store.PutVehicle(ctx, vehicle)
	}

	return nil
}

func containsDemoID(ids []string, target string) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}
