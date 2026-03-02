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
			id, type, status, resource_id, message, error_message, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (id) DO UPDATE SET
			type=$2, status=$3, resource_id=$4, message=$5, error_message=$6, updated_at=$8
	`,
		op.ID, op.Type, op.Status, op.ResourceID, op.Message, op.ErrorMessage, op.CreatedAt, op.UpdatedAt,
	)
	return err
}

func (s *PGStore) Get(ctx context.Context, id string) (*Operation, error) {
	var op Operation
	if err := s.pool.QueryRow(ctx, `
		SELECT id, type, status, resource_id, message, error_message, created_at, updated_at
		FROM operations
		WHERE id=$1
	`, id).Scan(
		&op.ID,
		&op.Type,
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
