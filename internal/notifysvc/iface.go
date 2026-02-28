package notifysvc

import "context"

// NotifStore defines the storage interface for notification persistence.
// Implemented by NATSStore (NATS KV) and PGStore (PostgreSQL).
type NotifStore interface {
	Put(ctx context.Context, n *Notification) error
	Get(ctx context.Context, userID, notifID string) (*Notification, error)
	ListForUser(ctx context.Context, userID string, unreadOnly bool) ([]Notification, error)
	ListForFleet(ctx context.Context, fleetID string, unreadOnly bool) ([]Notification, error)
	CountUnread(ctx context.Context, userID string) (int, error)
	Delete(ctx context.Context, userID, notifID string) error
}
