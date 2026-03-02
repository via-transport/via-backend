package opsvc

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

func (s *KVStore) Put(ctx context.Context, op *Operation) error {
	data, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("marshal operation: %w", err)
	}
	_, err = s.kv.Put(ctx, op.ID, data)
	return err
}

func (s *KVStore) Get(ctx context.Context, id string) (*Operation, error) {
	entry, err := s.kv.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	var op Operation
	if err := json.Unmarshal(entry.Value(), &op); err != nil {
		return nil, err
	}
	return &op, nil
}

func (s *KVStore) FindByIdempotencyKey(ctx context.Context, key string) (*Operation, error) {
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "no keys found") {
			return nil, fmt.Errorf("operation not found")
		}
		return nil, err
	}
	for _, candidate := range keys {
		entry, err := s.kv.Get(ctx, candidate)
		if err != nil {
			continue
		}
		var op Operation
		if err := json.Unmarshal(entry.Value(), &op); err != nil {
			continue
		}
		if op.IdempotencyKey == key {
			return &op, nil
		}
	}
	return nil, fmt.Errorf("operation not found")
}
