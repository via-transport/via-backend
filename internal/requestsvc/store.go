package requestsvc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

type KVStore struct {
	kv jetstream.KeyValue
}

func NewStore(kv jetstream.KeyValue) *KVStore {
	return &KVStore{kv: kv}
}

var _ Store = (*KVStore)(nil)

func (s *KVStore) Put(ctx context.Context, req *DriverRequest) error {
	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal driver request: %w", err)
	}
	_, err = s.kv.Put(ctx, req.ID, data)
	return err
}

func (s *KVStore) Get(ctx context.Context, id string) (*DriverRequest, error) {
	entry, err := s.kv.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	var req DriverRequest
	if err := json.Unmarshal(entry.Value(), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func (s *KVStore) List(ctx context.Context, fleetID, status string) ([]DriverRequest, error) {
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "no keys found") {
			return nil, nil
		}
		return nil, err
	}
	items := make([]DriverRequest, 0, len(keys))
	for _, key := range keys {
		entry, err := s.kv.Get(ctx, key)
		if err != nil {
			continue
		}
		var req DriverRequest
		if err := json.Unmarshal(entry.Value(), &req); err != nil {
			continue
		}
		if fleetID != "" && req.FleetID != fleetID {
			continue
		}
		if status != "" && req.Status != status {
			continue
		}
		items = append(items, req)
	}
	return items, nil
}

func (s *KVStore) FindPendingByUser(ctx context.Context, fleetID, userID string) (*DriverRequest, error) {
	items, err := s.List(ctx, fleetID, StatusPending)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if item.UserID == userID {
			copy := item
			return &copy, nil
		}
	}
	return nil, fmt.Errorf("driver request not found")
}
