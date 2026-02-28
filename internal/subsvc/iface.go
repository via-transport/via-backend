package subsvc

import "context"

// SubStore defines the storage interface for subscription persistence.
// Implemented by NATSStore (NATS KV) and PGStore (PostgreSQL).
type SubStore interface {
	Put(ctx context.Context, sub *Subscription) error
	Get(ctx context.Context, userID, subID string) (*Subscription, error)
	ListForUser(ctx context.Context, userID string) ([]Subscription, error)
	ListForVehicle(ctx context.Context, vehicleID string) ([]Subscription, error)
	Delete(ctx context.Context, userID, subID string) error
}
