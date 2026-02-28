package subsvc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// Store persists subscriptions in a NATS KV bucket.
type Store struct {
	kv jetstream.KeyValue
}

// NewStore creates a subscription store.
func NewStore(kv jetstream.KeyValue) *Store {
	return &Store{kv: kv}
}

// Put creates or updates a subscription. Key format: user_id.sub_id
func (s *Store) Put(ctx context.Context, sub *Subscription) error {
	data, err := json.Marshal(sub)
	if err != nil {
		return fmt.Errorf("marshal subscription: %w", err)
	}
	key := sub.UserID + "." + sub.ID
	_, err = s.kv.Put(ctx, key, data)
	return err
}

// Get retrieves a subscription.
func (s *Store) Get(ctx context.Context, userID, subID string) (*Subscription, error) {
	entry, err := s.kv.Get(ctx, userID+"."+subID)
	if err != nil {
		return nil, err
	}
	var sub Subscription
	if err := json.Unmarshal(entry.Value(), &sub); err != nil {
		return nil, err
	}
	return &sub, nil
}

// ListForUser returns all subscriptions for a user.
func (s *Store) ListForUser(ctx context.Context, userID string) ([]Subscription, error) {
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		if err.Error() == "nats: no keys found" {
			return nil, nil
		}
		return nil, err
	}
	prefix := userID + "."
	var result []Subscription
	for _, k := range keys {
		if len(k) <= len(prefix) || k[:len(prefix)] != prefix {
			continue
		}
		entry, err := s.kv.Get(ctx, k)
		if err != nil {
			continue
		}
		var sub Subscription
		if err := json.Unmarshal(entry.Value(), &sub); err != nil {
			continue
		}
		result = append(result, sub)
	}
	return result, nil
}

// ListForVehicle returns all active subscriptions for a vehicle across all users.
func (s *Store) ListForVehicle(ctx context.Context, vehicleID string) ([]Subscription, error) {
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		if err.Error() == "nats: no keys found" {
			return nil, nil
		}
		return nil, err
	}
	var result []Subscription
	for _, k := range keys {
		entry, err := s.kv.Get(ctx, k)
		if err != nil {
			continue
		}
		var sub Subscription
		if err := json.Unmarshal(entry.Value(), &sub); err != nil {
			continue
		}
		if sub.VehicleID == vehicleID && sub.Status == "active" {
			result = append(result, sub)
		}
	}
	return result, nil
}

// Delete removes a subscription.
func (s *Store) Delete(ctx context.Context, userID, subID string) error {
	return s.kv.Delete(ctx, userID+"."+subID)
}
