package authsvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// Store persists users in a NATS KV bucket.
type Store struct {
	users  jetstream.KeyValue // VIA_USERS bucket (key = user_id)
	emails jetstream.KeyValue // VIA_USER_EMAILS bucket (key = email → user_id)
}

// NewStore creates an auth store backed by the given KV buckets.
func NewStore(users, emails jetstream.KeyValue) *Store {
	return &Store{users: users, emails: emails}
}

// CreateUser stores a new user. Returns error if email already exists.
func (s *Store) CreateUser(ctx context.Context, user *User) error {
	emailKey := normalizeEmail(user.Email)

	// Check uniqueness via email index.
	if _, err := s.emails.Get(ctx, emailKey); err == nil {
		return errors.New("email already registered")
	}

	data, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("marshal user: %w", err)
	}

	if _, err := s.users.Put(ctx, user.ID, data); err != nil {
		return fmt.Errorf("put user: %w", err)
	}

	// Email → ID index.
	if _, err := s.emails.Put(ctx, emailKey, []byte(user.ID)); err != nil {
		// Best-effort rollback.
		_ = s.users.Delete(ctx, user.ID)
		return fmt.Errorf("put email index: %w", err)
	}

	return nil
}

// GetUser returns a user by ID.
func (s *Store) GetUser(ctx context.Context, userID string) (*User, error) {
	entry, err := s.users.Get(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("user not found: %w", err)
	}
	var user User
	if err := json.Unmarshal(entry.Value(), &user); err != nil {
		return nil, fmt.Errorf("unmarshal user: %w", err)
	}
	return &user, nil
}

// GetUserByEmail looks up user by email (via the email index).
func (s *Store) GetUserByEmail(ctx context.Context, email string) (*User, error) {
	emailKey := normalizeEmail(email)
	entry, err := s.emails.Get(ctx, emailKey)
	if err != nil {
		return nil, fmt.Errorf("email not found: %w", err)
	}
	return s.GetUser(ctx, string(entry.Value()))
}

// UpdateUser overwrites the user record.
func (s *Store) UpdateUser(ctx context.Context, user *User) error {
	data, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("marshal user: %w", err)
	}
	if _, err := s.users.Put(ctx, user.ID, data); err != nil {
		return fmt.Errorf("put user: %w", err)
	}
	return nil
}

// ListUsers returns all users (for admin use). Optionally filter by role/fleet.
func (s *Store) ListUsers(ctx context.Context, filterRole, filterFleet string) ([]User, error) {
	keys, err := s.users.Keys(ctx)
	if err != nil {
		if isNoKeys(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list user keys: %w", err)
	}

	users := make([]User, 0, len(keys))
	for _, k := range keys {
		entry, err := s.users.Get(ctx, k)
		if err != nil {
			continue
		}
		var u User
		if json.Unmarshal(entry.Value(), &u) != nil {
			continue
		}
		if filterRole != "" && u.Role != filterRole {
			continue
		}
		if filterFleet != "" && u.FleetID != filterFleet {
			continue
		}
		u.PasswordHash = "" // never expose
		users = append(users, u)
	}
	return users, nil
}

func normalizeEmail(email string) string {
	// NATS KV keys allow: [-/_=.a-zA-Z0-9]
	// Replace @ with _at_ for key compatibility.
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(email)), "@", "_at_")
}

func isNoKeys(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no keys found")
}
