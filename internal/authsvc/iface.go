package authsvc

import "context"

// UserStore defines the storage interface for user persistence.
// Implemented by NATSStore (NATS KV) and PGStore (PostgreSQL).
type UserStore interface {
	CreateUser(ctx context.Context, user *User) error
	GetUser(ctx context.Context, userID string) (*User, error)
	GetUserByEmail(ctx context.Context, email string) (*User, error)
	UpdateUser(ctx context.Context, user *User) error
	ListUsers(ctx context.Context, filterRole, filterFleet string) ([]User, error)
}
