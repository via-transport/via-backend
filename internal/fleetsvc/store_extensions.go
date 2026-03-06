package fleetsvc

import (
	"context"
	"errors"
)

var ErrVehicleLimitReached = errors.New("vehicle limit reached for current tenant plan")

// VehicleLimitEnforcer is an optional store extension for backends that can
// enforce vehicle-capacity checks atomically with the write.
type VehicleLimitEnforcer interface {
	CreateVehicleIfWithinLimit(ctx context.Context, v *Vehicle, vehicleLimit int) error
}
