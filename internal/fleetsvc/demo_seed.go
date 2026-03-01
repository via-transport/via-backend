package fleetsvc

import (
	"context"
	"time"
)

const (
	demoPrimaryVehicleID         = "veh_001"
	demoStandbyVehicleID         = "veh_002"
	demoDriverID                 = "drv_001"
	demoPrimaryVehicleRegNo      = "ND-4521"
	demoPrimaryVehicleType       = "bus"
	demoPrimaryVehicleService    = "school_transport"
	demoPrimaryVehicleStatus     = "en_route"
	demoPrimaryVehicleStatusNote = "Running on schedule"
	demoPrimaryRouteID           = "route_010"
	demoStandbyVehicleRegNo      = "CB-2408"
	demoStandbyVehicleType       = "van"
	demoStandbyVehicleService    = "corporate_shuttle"
	demoStandbyVehicleStatus     = "on_time"
	demoStandbyVehicleStatusNote = "Standby vehicle ready"
	demoStandbyRouteID           = "route_204"
	demoDriverName               = "Kamal Perera"
	demoDriverEmail              = "kamal.perera@via.local"
	demoDriverPhone              = "+94770001122"
)

// EnsureDemoFleet provisions a stable demo fleet in the backend so all apps
// can point at the same shared records instead of local-only mock data.
func EnsureDemoFleet(ctx context.Context, store FleetStore, fleetID string) error {
	if fleetID == "" {
		return nil
	}

	now := time.Now().UTC()

	driver, err := ensureDemoDriver(ctx, store, fleetID, now)
	if err != nil {
		return err
	}

	if err := ensurePrimaryDemoVehicle(ctx, store, fleetID, now, driver); err != nil {
		return err
	}

	if err := ensureStandbyDemoVehicle(ctx, store, fleetID, now); err != nil {
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
			AssignedVehicleIDs: []string{demoPrimaryVehicleID},
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
	if !containsDemoID(driver.AssignedVehicleIDs, demoPrimaryVehicleID) {
		driver.AssignedVehicleIDs = append(
			driver.AssignedVehicleIDs,
			demoPrimaryVehicleID,
		)
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

func ensurePrimaryDemoVehicle(
	ctx context.Context,
	store FleetStore,
	fleetID string,
	now time.Time,
	driver *Driver,
) error {
	vehicle, err := store.GetVehicle(ctx, fleetID, demoPrimaryVehicleID)
	if err != nil {
		vehicle = &Vehicle{
			ID:                 demoPrimaryVehicleID,
			RegistrationNumber: demoPrimaryVehicleRegNo,
			Type:               demoPrimaryVehicleType,
			ServiceType:        demoPrimaryVehicleService,
			IsActive:           true,
			Status:             demoPrimaryVehicleStatus,
			StatusMessage:      demoPrimaryVehicleStatusNote,
			CurrentRouteID:     demoPrimaryRouteID,
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
		vehicle.RegistrationNumber = demoPrimaryVehicleRegNo
		changed = true
	}
	if vehicle.Type == "" {
		vehicle.Type = demoPrimaryVehicleType
		changed = true
	}
	if shouldNormalizeServiceType(vehicle.ServiceType, demoPrimaryVehicleService, "school") {
		vehicle.ServiceType = demoPrimaryVehicleService
		changed = true
	}
	if vehicle.Status == "" {
		vehicle.Status = demoPrimaryVehicleStatus
		changed = true
	}
	if vehicle.StatusMessage == "" {
		vehicle.StatusMessage = demoPrimaryVehicleStatusNote
		changed = true
	}
	if vehicle.CurrentRouteID == "" {
		vehicle.CurrentRouteID = demoPrimaryRouteID
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

func ensureStandbyDemoVehicle(
	ctx context.Context,
	store FleetStore,
	fleetID string,
	now time.Time,
) error {
	vehicle, err := store.GetVehicle(ctx, fleetID, demoStandbyVehicleID)
	if err != nil {
		vehicle = &Vehicle{
			ID:                 demoStandbyVehicleID,
			RegistrationNumber: demoStandbyVehicleRegNo,
			Type:               demoStandbyVehicleType,
			ServiceType:        demoStandbyVehicleService,
			IsActive:           true,
			Status:             demoStandbyVehicleStatus,
			StatusMessage:      demoStandbyVehicleStatusNote,
			CurrentRouteID:     demoStandbyRouteID,
			FleetID:            fleetID,
			Capacity:           18,
			CurrentLocation: &VehicleLocation{
				Latitude:  6.9102,
				Longitude: 79.8848,
				Heading:   180,
				Speed:     0,
				Timestamp: now,
			},
			LastUpdated: now,
			CreatedAt:   now,
		}
		return store.PutVehicle(ctx, vehicle)
	}

	changed := false
	if vehicle.RegistrationNumber == "" {
		vehicle.RegistrationNumber = demoStandbyVehicleRegNo
		changed = true
	}
	if vehicle.Type == "" {
		vehicle.Type = demoStandbyVehicleType
		changed = true
	}
	if shouldNormalizeServiceType(
		vehicle.ServiceType,
		demoStandbyVehicleService,
		"office",
		"corporate",
	) {
		vehicle.ServiceType = demoStandbyVehicleService
		changed = true
	}
	if vehicle.Status == "" {
		vehicle.Status = demoStandbyVehicleStatus
		changed = true
	}
	if vehicle.StatusMessage == "" {
		vehicle.StatusMessage = demoStandbyVehicleStatusNote
		changed = true
	}
	if vehicle.CurrentRouteID == "" {
		vehicle.CurrentRouteID = demoStandbyRouteID
		changed = true
	}
	if !vehicle.IsActive {
		vehicle.IsActive = true
		changed = true
	}
	if vehicle.Capacity <= 0 {
		vehicle.Capacity = 18
		changed = true
	}
	if vehicle.CurrentLocation == nil {
		vehicle.CurrentLocation = &VehicleLocation{
			Latitude:  6.9102,
			Longitude: 79.8848,
			Heading:   180,
			Speed:     0,
			Timestamp: now,
		}
		changed = true
	}
	if vehicle.DriverID != "" || vehicle.DriverName != "" || vehicle.DriverPhone != "" {
		vehicle.DriverID = ""
		vehicle.DriverName = ""
		vehicle.DriverPhone = ""
		changed = true
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

func shouldNormalizeServiceType(current, desired string, aliases ...string) bool {
	if current == desired {
		return false
	}
	if current == "" {
		return true
	}
	for _, alias := range aliases {
		if current == alias {
			return true
		}
	}
	return false
}

func containsDemoID(ids []string, target string) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}
