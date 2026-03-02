package tenantsvc

import (
	"context"
	"errors"
	"fmt"
	"time"

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

func (s *PGStore) Put(ctx context.Context, tenant *Tenant) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO tenants (
			id, name, plan_type, subscription_status, trial_started_at, trial_ends_at, grace_ends_at,
			vehicle_limit, passenger_limit, driver_limit, location_publish_interval_seconds,
			event_hourly_limit, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (id) DO UPDATE SET
			name=$2,
			plan_type=$3,
			subscription_status=$4,
			trial_started_at=$5,
			trial_ends_at=$6,
			grace_ends_at=$7,
			vehicle_limit=$8,
			passenger_limit=$9,
			driver_limit=$10,
			location_publish_interval_seconds=$11,
			event_hourly_limit=$12,
			updated_at=$14
	`,
		tenant.ID, tenant.Name, tenant.PlanType, tenant.SubscriptionStatus,
		nilTime(tenant.TrialStartedAt), nilTime(tenant.TrialEndsAt), nilTime(tenant.GraceEndsAt),
		tenant.VehicleLimit, tenant.PassengerLimit, tenant.DriverLimit,
		tenant.LocationPublishIntervalS, tenant.EventHourlyLimit,
		tenant.CreatedAt, tenant.UpdatedAt,
	)
	return err
}

func (s *PGStore) Get(ctx context.Context, tenantID string) (*Tenant, error) {
	return s.scanOne(s.pool.QueryRow(ctx, `
		SELECT id, name, plan_type, subscription_status, trial_started_at, trial_ends_at, grace_ends_at,
		       vehicle_limit, passenger_limit, driver_limit, location_publish_interval_seconds,
		       event_hourly_limit, created_at, updated_at
		FROM tenants
		WHERE id=$1
	`, tenantID))
}

func (s *PGStore) List(ctx context.Context) ([]Tenant, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, plan_type, subscription_status, trial_started_at, trial_ends_at, grace_ends_at,
		       vehicle_limit, passenger_limit, driver_limit, location_publish_interval_seconds,
		       event_hourly_limit, created_at, updated_at
		FROM tenants
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []Tenant
	for rows.Next() {
		tenant, err := scanTenantRows(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *tenant)
	}
	return items, rows.Err()
}

func (s *PGStore) scanOne(row pgx.Row) (*Tenant, error) {
	tenant, err := scanTenantRows(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("tenant not found")
		}
		return nil, err
	}
	return tenant, nil
}

type tenantScanner interface {
	Scan(dest ...any) error
}

func scanTenantRows(row tenantScanner) (*Tenant, error) {
	var tenant Tenant
	var trialStartedAt *time.Time
	var trialEndsAt *time.Time
	var graceEndsAt *time.Time
	err := row.Scan(
		&tenant.ID, &tenant.Name, &tenant.PlanType, &tenant.SubscriptionStatus,
		&trialStartedAt, &trialEndsAt, &graceEndsAt,
		&tenant.VehicleLimit, &tenant.PassengerLimit, &tenant.DriverLimit,
		&tenant.LocationPublishIntervalS, &tenant.EventHourlyLimit,
		&tenant.CreatedAt, &tenant.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	if trialStartedAt != nil {
		tenant.TrialStartedAt = *trialStartedAt
	}
	if trialEndsAt != nil {
		tenant.TrialEndsAt = *trialEndsAt
	}
	if graceEndsAt != nil {
		tenant.GraceEndsAt = *graceEndsAt
	}
	return &tenant, nil
}

func nilTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
