package requestsvc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

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
	req.RequestType = defaultDriverRequestType(req.RequestType, req.VehicleID)
	return &req, nil
}

func (s *KVStore) List(ctx context.Context, fleetID, status, requestType string) ([]DriverRequest, error) {
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
		req.RequestType = defaultDriverRequestType(req.RequestType, req.VehicleID)
		if fleetID != "" && req.FleetID != fleetID {
			continue
		}
		if status != "" && req.Status != status {
			continue
		}
		if requestType != "" && strings.TrimSpace(req.RequestType) != requestType {
			continue
		}
		items = append(items, req)
	}
	return items, nil
}

func (s *KVStore) FindPendingByUser(ctx context.Context, fleetID, userID, requestType string) (*DriverRequest, error) {
	items, err := s.List(ctx, fleetID, StatusPending, requestType)
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

func (s *KVStore) RevokeApprovedVehicleAssignments(ctx context.Context, fleetID, userID string) (int, error) {
	normalizedFleetID := strings.TrimSpace(fleetID)
	normalizedUserID := strings.TrimSpace(userID)
	if normalizedFleetID == "" || normalizedUserID == "" {
		return 0, nil
	}

	items, err := s.List(ctx, normalizedFleetID, StatusApproved, RequestTypeVehicleAssignment)
	if err != nil {
		return 0, err
	}

	now := time.Now().UTC()
	revokedCount := 0
	for i := range items {
		item := items[i]
		if strings.TrimSpace(item.UserID) != normalizedUserID {
			continue
		}
		item.Status = StatusRevoked
		item.UpdatedAt = now
		if err := s.Put(ctx, &item); err != nil {
			return revokedCount, err
		}
		revokedCount++
	}

	return revokedCount, nil
}
