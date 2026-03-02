package opsvc

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGStore struct {
	pool *pgxpool.Pool
}

func NewPGStore(pool *pgxpool.Pool) *PGStore {
	return &PGStore{pool: pool}
}

var _ Store = (*PGStore)(nil)

func (s *PGStore) Put(ctx context.Context, op *Operation) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO operations (
			id, type, idempotency_key, status, resource_id, message, error_message, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (id) DO UPDATE SET
			type=$2, idempotency_key=$3, status=$4, resource_id=$5, message=$6, error_message=$7, updated_at=$9
	`,
		op.ID, op.Type, op.IdempotencyKey, op.Status, op.ResourceID, op.Message, op.ErrorMessage, op.CreatedAt, op.UpdatedAt,
	)
	return err
}

func (s *PGStore) Get(ctx context.Context, id string) (*Operation, error) {
	var op Operation
	if err := s.pool.QueryRow(ctx, `
		SELECT id, type, idempotency_key, status, resource_id, message, error_message, created_at, updated_at
		FROM operations
		WHERE id=$1
	`, id).Scan(
		&op.ID,
		&op.Type,
		&op.IdempotencyKey,
		&op.Status,
		&op.ResourceID,
		&op.Message,
		&op.ErrorMessage,
		&op.CreatedAt,
		&op.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("operation not found")
		}
		return nil, err
	}
	return &op, nil
}

func (s *PGStore) FindByIdempotencyKey(ctx context.Context, key string) (*Operation, error) {
	var op Operation
	if err := s.pool.QueryRow(ctx, `
		SELECT id, type, idempotency_key, status, resource_id, message, error_message, created_at, updated_at
		FROM operations
		WHERE idempotency_key=$1
		ORDER BY created_at DESC
		LIMIT 1
	`, key).Scan(
		&op.ID,
		&op.Type,
		&op.IdempotencyKey,
		&op.Status,
		&op.ResourceID,
		&op.Message,
		&op.ErrorMessage,
		&op.CreatedAt,
		&op.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("operation not found")
		}
		return nil, err
	}
	return &op, nil
}
