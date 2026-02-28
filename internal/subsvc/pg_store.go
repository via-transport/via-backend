package subsvc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGStore implements SubStore using PostgreSQL.
type PGStore struct {
	pool *pgxpool.Pool
}

// NewPGStore creates a subscription store backed by PostgreSQL.
func NewPGStore(pool *pgxpool.Pool) *PGStore {
	return &PGStore{pool: pool}
}

// Compile-time interface check.
var _ SubStore = (*PGStore)(nil)

func (s *PGStore) Put(ctx context.Context, sub *Subscription) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO subscriptions (id, user_id, vehicle_id, fleet_id, status,
		  pref_notify_arrival, pref_notify_delay, pref_notify_event, pref_radius_meters,
		  created_at, updated_at, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (id) DO UPDATE SET
		  user_id=$2, vehicle_id=$3, fleet_id=$4, status=$5,
		  pref_notify_arrival=$6, pref_notify_delay=$7, pref_notify_event=$8, pref_radius_meters=$9,
		  updated_at=$11, expires_at=$12
	`,
		sub.ID, sub.UserID, sub.VehicleID, sub.FleetID, sub.Status,
		sub.Preferences.NotifyOnArrival, sub.Preferences.NotifyOnDelay,
		sub.Preferences.NotifyOnEvent, sub.Preferences.RadiusMeters,
		sub.CreatedAt, sub.UpdatedAt, nilTimeSub(sub.ExpiresAt),
	)
	return err
}

func (s *PGStore) Get(ctx context.Context, userID, subID string) (*Subscription, error) {
	return s.scanSub(s.pool.QueryRow(ctx, subSelectSQL+" WHERE id=$1 AND user_id=$2", subID, userID))
}

func (s *PGStore) ListForUser(ctx context.Context, userID string) ([]Subscription, error) {
	return s.querySubs(ctx, subSelectSQL+" WHERE user_id=$1 ORDER BY created_at DESC", userID)
}

func (s *PGStore) ListForVehicle(ctx context.Context, vehicleID string) ([]Subscription, error) {
	return s.querySubs(ctx, subSelectSQL+" WHERE vehicle_id=$1 AND status='active' ORDER BY created_at DESC", vehicleID)
}

func (s *PGStore) Delete(ctx context.Context, userID, subID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM subscriptions WHERE id=$1 AND user_id=$2`, subID, userID)
	return err
}

const subSelectSQL = `SELECT id, user_id, vehicle_id, fleet_id, status,
	pref_notify_arrival, pref_notify_delay, pref_notify_event, pref_radius_meters,
	created_at, updated_at, expires_at
	FROM subscriptions`

func (s *PGStore) scanSub(row pgx.Row) (*Subscription, error) {
	var sub Subscription
	var expiresAt *time.Time
	err := row.Scan(
		&sub.ID, &sub.UserID, &sub.VehicleID, &sub.FleetID, &sub.Status,
		&sub.Preferences.NotifyOnArrival, &sub.Preferences.NotifyOnDelay,
		&sub.Preferences.NotifyOnEvent, &sub.Preferences.RadiusMeters,
		&sub.CreatedAt, &sub.UpdatedAt, &expiresAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("subscription not found")
		}
		return nil, err
	}
	if expiresAt != nil {
		sub.ExpiresAt = *expiresAt
	}
	return &sub, nil
}

func (s *PGStore) querySubs(ctx context.Context, query string, args ...interface{}) ([]Subscription, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Subscription
	for rows.Next() {
		var sub Subscription
		var expiresAt *time.Time
		if err := rows.Scan(
			&sub.ID, &sub.UserID, &sub.VehicleID, &sub.FleetID, &sub.Status,
			&sub.Preferences.NotifyOnArrival, &sub.Preferences.NotifyOnDelay,
			&sub.Preferences.NotifyOnEvent, &sub.Preferences.RadiusMeters,
			&sub.CreatedAt, &sub.UpdatedAt, &expiresAt,
		); err != nil {
			return nil, err
		}
		if expiresAt != nil {
			sub.ExpiresAt = *expiresAt
		}
		result = append(result, sub)
	}
	return result, rows.Err()
}

func nilTimeSub(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t
}
