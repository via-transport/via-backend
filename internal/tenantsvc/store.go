package tenantsvc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

type StoreKV struct {
	kv jetstream.KeyValue
}

func NewStore(kv jetstream.KeyValue) *StoreKV {
	return &StoreKV{kv: kv}
}

var _ Store = (*StoreKV)(nil)

func (s *StoreKV) Put(ctx context.Context, tenant *Tenant) error {
	data, err := json.Marshal(tenant)
	if err != nil {
		return fmt.Errorf("marshal tenant: %w", err)
	}
	_, err = s.kv.Put(ctx, tenant.ID, data)
	return err
}

func (s *StoreKV) Get(ctx context.Context, tenantID string) (*Tenant, error) {
	entry, err := s.kv.Get(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	var tenant Tenant
	if err := json.Unmarshal(entry.Value(), &tenant); err != nil {
		return nil, err
	}
	return &tenant, nil
}

func (s *StoreKV) List(ctx context.Context) ([]Tenant, error) {
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "no keys found") {
			return nil, nil
		}
		return nil, err
	}

	items := make([]Tenant, 0, len(keys))
	for _, key := range keys {
		entry, err := s.kv.Get(ctx, key)
		if err != nil {
			continue
		}
		var tenant Tenant
		if err := json.Unmarshal(entry.Value(), &tenant); err != nil {
			continue
		}
		items = append(items, tenant)
	}
	return items, nil
}
