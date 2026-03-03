package opsvc

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
			id, type, fleet_id, idempotency_key, status, resource_id, message, error_message, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (id) DO UPDATE SET
			type=$2, fleet_id=$3, idempotency_key=$4, status=$5, resource_id=$6, message=$7, error_message=$8, updated_at=$10
	`,
		op.ID, op.Type, op.FleetID, op.IdempotencyKey, op.Status, op.ResourceID, op.Message, op.ErrorMessage, op.CreatedAt, op.UpdatedAt,
	)
	return err
}

func (s *PGStore) Get(ctx context.Context, id string) (*Operation, error) {
	var op Operation
	if err := s.pool.QueryRow(ctx, `
		SELECT id, type, fleet_id, idempotency_key, status, resource_id, message, error_message, created_at, updated_at
		FROM operations
		WHERE id=$1
	`, id).Scan(
		&op.ID,
		&op.Type,
		&op.FleetID,
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

func (s *PGStore) List(ctx context.Context, filter ListFilter) ([]Operation, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 20
	}

	query := `
		SELECT id, type, fleet_id, idempotency_key, status, resource_id, message, error_message, created_at, updated_at
		FROM operations
	`
	clauses := make([]string, 0, 3)
	args := make([]any, 0, 4)
	argIndex := 1

	if filter.Type != "" {
		clauses = append(clauses, fmt.Sprintf("type=$%d", argIndex))
		args = append(args, filter.Type)
		argIndex++
	}
	if filter.Status != "" {
		clauses = append(clauses, fmt.Sprintf("status=$%d", argIndex))
		args = append(args, filter.Status)
		argIndex++
	}
	if filter.FleetID != "" {
		clauses = append(clauses, fmt.Sprintf("fleet_id=$%d", argIndex))
		args = append(args, filter.FleetID)
		argIndex++
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d", argIndex)
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]Operation, 0, limit)
	for rows.Next() {
		var op Operation
		if err := rows.Scan(
			&op.ID,
			&op.Type,
			&op.FleetID,
			&op.IdempotencyKey,
			&op.Status,
			&op.ResourceID,
			&op.Message,
			&op.ErrorMessage,
			&op.CreatedAt,
			&op.UpdatedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, op)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return items, nil
}

func (s *PGStore) FindByIdempotencyKey(ctx context.Context, key string) (*Operation, error) {
	var op Operation
	if err := s.pool.QueryRow(ctx, `
		SELECT id, type, fleet_id, idempotency_key, status, resource_id, message, error_message, created_at, updated_at
		FROM operations
		WHERE idempotency_key=$1
		ORDER BY created_at DESC
		LIMIT 1
	`, key).Scan(
		&op.ID,
		&op.Type,
		&op.FleetID,
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
