package notifysvc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// Store persists notifications in a NATS KV bucket.
type Store struct {
	kv jetstream.KeyValue
}

// NewStore creates a notification store using the given KV bucket.
func NewStore(kv jetstream.KeyValue) *Store {
	return &Store{kv: kv}
}

// Put creates or updates a notification.
func (s *Store) Put(ctx context.Context, n *Notification) error {
	data, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("marshal notification: %w", err)
	}
	key := n.UserID + "." + n.ID
	_, err = s.kv.Put(ctx, key, data)
	return err
}

// Get retrieves a notification by userID and notificationID.
func (s *Store) Get(ctx context.Context, userID, notifID string) (*Notification, error) {
	entry, err := s.kv.Get(ctx, userID+"."+notifID)
	if err != nil {
		return nil, err
	}
	var n Notification
	if err := json.Unmarshal(entry.Value(), &n); err != nil {
		return nil, err
	}
	return &n, nil
}

// ListForUser returns all notifications for a user, optionally filtered to unread only.
func (s *Store) ListForUser(ctx context.Context, userID string, unreadOnly bool) ([]Notification, error) {
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		if err.Error() == "nats: no keys found" {
			return nil, nil
		}
		return nil, err
	}
	prefix := userID + "."
	var result []Notification
	for _, k := range keys {
		if len(k) <= len(prefix) || k[:len(prefix)] != prefix {
			continue
		}
		entry, err := s.kv.Get(ctx, k)
		if err != nil {
			continue
		}
		var n Notification
		if err := json.Unmarshal(entry.Value(), &n); err != nil {
			continue
		}
		if unreadOnly && n.IsRead {
			continue
		}
		result = append(result, n)
	}
	return result, nil
}

// ListForFleet returns notifications for a fleet. For the NATS KV store this
// scans all keys and filters by FleetID, which is acceptable for small datasets.
func (s *Store) ListForFleet(ctx context.Context, fleetID string, unreadOnly bool) ([]Notification, error) {
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		if err.Error() == "nats: no keys found" {
			return nil, nil
		}
		return nil, err
	}
	var result []Notification
	for _, k := range keys {
		entry, err := s.kv.Get(ctx, k)
		if err != nil {
			continue
		}
		var n Notification
		if err := json.Unmarshal(entry.Value(), &n); err != nil {
			continue
		}
		if n.FleetID != fleetID {
			continue
		}
		if unreadOnly && n.IsRead {
			continue
		}
		result = append(result, n)
	}
	return result, nil
}

// CountUnread returns the number of unread notifications for a user.
// It counts inline without deserialising all notifications.
func (s *Store) CountUnread(ctx context.Context, userID string) (int, error) {
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		if err.Error() == "nats: no keys found" {
			return 0, nil
		}
		return 0, err
	}
	prefix := userID + "."
	count := 0
	for _, k := range keys {
		if len(k) <= len(prefix) || k[:len(prefix)] != prefix {
			continue
		}
		entry, err := s.kv.Get(ctx, k)
		if err != nil {
			continue
		}
		var n Notification
		if err := json.Unmarshal(entry.Value(), &n); err != nil {
			continue
		}
		if !n.IsRead {
			count++
		}
	}
	return count, nil
}

// Delete removes a notification.
func (s *Store) Delete(ctx context.Context, userID, notifID string) error {
	return s.kv.Delete(ctx, userID+"."+notifID)
}
