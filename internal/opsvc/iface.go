package opsvc

import "context"

type Store interface {
	Put(ctx context.Context, op *Operation) error
	Get(ctx context.Context, id string) (*Operation, error)
	FindByIdempotencyKey(ctx context.Context, key string) (*Operation, error)
}
