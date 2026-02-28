package notifysvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGStore implements NotifStore using PostgreSQL.
type PGStore struct {
	pool *pgxpool.Pool
}

// NewPGStore creates a notification store backed by PostgreSQL.
func NewPGStore(pool *pgxpool.Pool) *PGStore {
	return &PGStore{pool: pool}
}

// Compile-time interface check.
var _ NotifStore = (*PGStore)(nil)

func (s *PGStore) Put(ctx context.Context, n *Notification) error {
	dataJSON, _ := json.Marshal(n.Data)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO notifications (id, user_id, fleet_id, vehicle_id, type, title, body, data, is_read, created_at, read_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (id) DO UPDATE SET
		  user_id=$2, fleet_id=$3, vehicle_id=$4, type=$5, title=$6, body=$7,
		  data=$8, is_read=$9, read_at=$11
	`,
		n.ID, n.UserID, n.FleetID, n.VehicleID, n.Type, n.Title, n.Body,
		dataJSON, n.IsRead, n.CreatedAt, nilTimeNotif(n.ReadAt),
	)
	return err
}

func (s *PGStore) Get(ctx context.Context, userID, notifID string) (*Notification, error) {
	var n Notification
	var dataJSON []byte
	var readAt *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT id, user_id, fleet_id, vehicle_id, type, title, body, data, is_read, created_at, read_at
		FROM notifications WHERE id=$1 AND user_id=$2
	`, notifID, userID).Scan(
		&n.ID, &n.UserID, &n.FleetID, &n.VehicleID, &n.Type, &n.Title, &n.Body,
		&dataJSON, &n.IsRead, &n.CreatedAt, &readAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("notification not found")
		}
		return nil, err
	}
	if len(dataJSON) > 0 {
		_ = json.Unmarshal(dataJSON, &n.Data)
	}
	if readAt != nil {
		n.ReadAt = *readAt
	}
	return &n, nil
}

func (s *PGStore) ListForUser(ctx context.Context, userID string, unreadOnly bool) ([]Notification, error) {
	query := `SELECT id, user_id, fleet_id, vehicle_id, type, title, body, data, is_read, created_at, read_at
	           FROM notifications WHERE user_id=$1`
	if unreadOnly {
		query += " AND NOT is_read"
	}
	query += " ORDER BY created_at DESC"

	rows, err := s.pool.Query(ctx, query, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Notification
	for rows.Next() {
		var n Notification
		var dataJSON []byte
		var readAt *time.Time
		if err := rows.Scan(
			&n.ID, &n.UserID, &n.FleetID, &n.VehicleID, &n.Type, &n.Title, &n.Body,
			&dataJSON, &n.IsRead, &n.CreatedAt, &readAt,
		); err != nil {
			return nil, err
		}
		if len(dataJSON) > 0 {
			_ = json.Unmarshal(dataJSON, &n.Data)
		}
		if readAt != nil {
			n.ReadAt = *readAt
		}
		result = append(result, n)
	}
	return result, rows.Err()
}

func (s *PGStore) ListForFleet(ctx context.Context, fleetID string, unreadOnly bool) ([]Notification, error) {
	query := `SELECT id, user_id, fleet_id, vehicle_id, type, title, body, data, is_read, created_at, read_at
	           FROM notifications WHERE fleet_id=$1`
	if unreadOnly {
		query += " AND NOT is_read"
	}
	query += " ORDER BY created_at DESC LIMIT 200"

	rows, err := s.pool.Query(ctx, query, fleetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Notification
	for rows.Next() {
		var n Notification
		var dataJSON []byte
		var readAt *time.Time
		if err := rows.Scan(
			&n.ID, &n.UserID, &n.FleetID, &n.VehicleID, &n.Type, &n.Title, &n.Body,
			&dataJSON, &n.IsRead, &n.CreatedAt, &readAt,
		); err != nil {
			return nil, err
		}
		if len(dataJSON) > 0 {
			_ = json.Unmarshal(dataJSON, &n.Data)
		}
		if readAt != nil {
			n.ReadAt = *readAt
		}
		result = append(result, n)
	}
	return result, rows.Err()
}

func (s *PGStore) CountUnread(ctx context.Context, userID string) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM notifications WHERE user_id=$1 AND NOT is_read
	`, userID).Scan(&count)
	return count, err
}

func (s *PGStore) Delete(ctx context.Context, userID, notifID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM notifications WHERE id=$1 AND user_id=$2`, notifID, userID)
	return err
}

func nilTimeNotif(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t
}
