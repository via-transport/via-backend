package fleetsvc

import "context"

// FleetStore defines the storage interface for fleet service persistence.
// Implemented by NATSStore (NATS KV) and PGStore (PostgreSQL).
type FleetStore interface {
	// Vehicles
	PutVehicle(ctx context.Context, v *Vehicle) error
	GetVehicle(ctx context.Context, fleetID, vehicleID string) (*Vehicle, error)
	GetVehicleByID(ctx context.Context, vehicleID string) (*Vehicle, error)
	DeleteVehicle(ctx context.Context, fleetID, vehicleID string) error
	ListVehicles(ctx context.Context, fleetID string) ([]Vehicle, error)
	ListVehiclesForDriver(ctx context.Context, fleetID, driverID string) ([]Vehicle, error)

	// Drivers
	PutDriver(ctx context.Context, d *Driver) error
	GetDriver(ctx context.Context, fleetID, driverID string) (*Driver, error)
	DeleteDriver(ctx context.Context, fleetID, driverID string) error
	ListDrivers(ctx context.Context, fleetID string) ([]Driver, error)

	// Events
	PutEvent(ctx context.Context, e *SpecialEvent) error
	GetEvent(ctx context.Context, eventID string) (*SpecialEvent, error)
	ListEvents(ctx context.Context, fleetID, vehicleID string) ([]SpecialEvent, error)

	// Notices
	PutNotice(ctx context.Context, n *DriverNotice) error
	GetNotice(ctx context.Context, noticeID string) (*DriverNotice, error)
	ListNotices(ctx context.Context, fleetID, vehicleID, driverID string) ([]DriverNotice, error)
}
