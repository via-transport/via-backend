package authsvc

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"via-backend/internal/tenantsvc"
)

// PGStore implements UserStore using PostgreSQL.
type PGStore struct {
	pool *pgxpool.Pool
}

// NewPGStore creates an auth store backed by PostgreSQL.
func NewPGStore(pool *pgxpool.Pool) *PGStore {
	return &PGStore{pool: pool}
}

// Compile-time interface check.
var _ UserStore = (*PGStore)(nil)
var _ ownerTransactionalRegistrar = (*PGStore)(nil)
var _ ownerFleetTransactionalStore = (*PGStore)(nil)

func (s *PGStore) CreateUser(ctx context.Context, user *User) error {
	return s.insertUser(ctx, s.pool, user)
}

func (s *PGStore) CreateOwnerWithTenant(ctx context.Context, user *User, tenant *tenantsvc.Tenant) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin owner registration tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if _, err := tx.Exec(ctx, `
		INSERT INTO tenants (
			id, name, plan_type, subscription_status, trial_started_at, trial_ends_at, grace_ends_at,
			vehicle_limit, passenger_limit, driver_limit, location_publish_interval_seconds,
			event_hourly_limit, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
	`,
		tenant.ID, tenant.Name, tenant.PlanType, tenant.SubscriptionStatus,
		nilTenantTime(tenant.TrialStartedAt), nilTenantTime(tenant.TrialEndsAt), nilTenantTime(tenant.GraceEndsAt),
		tenant.VehicleLimit, tenant.PassengerLimit, tenant.DriverLimit,
		tenant.LocationPublishIntervalS, tenant.EventHourlyLimit,
		tenant.CreatedAt, tenant.UpdatedAt,
	); err != nil {
		if isUniqueViolation(err) {
			return errors.New("fleet already registered")
		}
		return fmt.Errorf("insert tenant: %w", err)
	}

	if err := s.insertUser(ctx, tx, user); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit owner registration: %w", err)
	}
	return nil
}

func (s *PGStore) SetupOwnerFleet(ctx context.Context, userID string, tenant *tenantsvc.Tenant) (*User, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin owner fleet tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	user, err := s.scanUser(tx.QueryRow(ctx, `
		SELECT id, email, password_hash, google_subject, display_name, phone, photo_url, workplace, address, employee_number,
		       role, fleet_id, vehicle_id, is_active, created_at, updated_at, last_login_at
		FROM users
		WHERE id = $1
		FOR UPDATE
	`, userID))
	if err != nil {
		return nil, err
	}
	if user.Role != "owner" {
		return nil, errors.New("only owners can create fleets")
	}
	if strings.TrimSpace(user.FleetID) != "" {
		return nil, errors.New("owner already linked to a fleet")
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO tenants (
			id, name, plan_type, subscription_status, trial_started_at, trial_ends_at, grace_ends_at,
			vehicle_limit, passenger_limit, driver_limit, location_publish_interval_seconds,
			event_hourly_limit, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
	`,
		tenant.ID, tenant.Name, tenant.PlanType, tenant.SubscriptionStatus,
		nilTenantTime(tenant.TrialStartedAt), nilTenantTime(tenant.TrialEndsAt), nilTenantTime(tenant.GraceEndsAt),
		tenant.VehicleLimit, tenant.PassengerLimit, tenant.DriverLimit,
		tenant.LocationPublishIntervalS, tenant.EventHourlyLimit,
		tenant.CreatedAt, tenant.UpdatedAt,
	); err != nil {
		if isUniqueViolation(err) {
			return nil, errors.New("fleet already registered")
		}
		return nil, fmt.Errorf("insert tenant: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO tenant_memberships (user_id, tenant_id, role, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5)
	`, user.ID, tenant.ID, "owner", tenant.CreatedAt, tenant.UpdatedAt); err != nil {
		return nil, fmt.Errorf("insert tenant membership: %w", err)
	}

	user.FleetID = tenant.ID
	user.UpdatedAt = tenant.UpdatedAt
	if _, err := tx.Exec(ctx, `
		UPDATE users SET fleet_id=$2, updated_at=$3
		WHERE id=$1
	`, user.ID, user.FleetID, user.UpdatedAt); err != nil {
		return nil, fmt.Errorf("update owner fleet: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit owner fleet: %w", err)
	}
	return user, nil
}

type userExec interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func (s *PGStore) insertUser(ctx context.Context, exec userExec, user *User) error {
	_, err := exec.Exec(ctx, `
		INSERT INTO users (id, email, password_hash, google_subject, display_name, phone, photo_url,
		                    workplace, address, employee_number, role, fleet_id, vehicle_id, is_active, created_at, updated_at, last_login_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
	`,
		user.ID, strings.ToLower(strings.TrimSpace(user.Email)),
		user.PasswordHash, user.GoogleSubject, user.DisplayName,
		user.Phone, user.PhotoURL, user.Workplace, user.Address, user.EmployeeNo,
		user.Role, user.FleetID, user.VehicleID, user.IsActive,
		user.CreatedAt, user.UpdatedAt, nilTime(user.LastLoginAt),
	)
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
			switch pgErr.ConstraintName {
			case "idx_users_google_subject":
				return errors.New("google account already linked")
			default:
				return errors.New("email already registered")
			}
		}
		if isUniqueViolation(err) {
			if strings.Contains(err.Error(), "google_subject") {
				return errors.New("google account already linked")
			}
			return errors.New("email already registered")
		}
		return fmt.Errorf("insert user: %w", err)
	}
	return nil
}

func (s *PGStore) GetUser(ctx context.Context, userID string) (*User, error) {
	return s.scanUser(s.pool.QueryRow(ctx, `
		SELECT id, email, password_hash, google_subject, display_name, phone, photo_url, workplace, address, employee_number,
		       role, fleet_id, vehicle_id, is_active, created_at, updated_at, last_login_at
		FROM users WHERE id = $1
	`, userID))
}

func (s *PGStore) GetUserByEmail(ctx context.Context, email string) (*User, error) {
	return s.scanUser(s.pool.QueryRow(ctx, `
		SELECT id, email, password_hash, google_subject, display_name, phone, photo_url, workplace, address, employee_number,
		       role, fleet_id, vehicle_id, is_active, created_at, updated_at, last_login_at
		FROM users WHERE email = $1
	`, strings.ToLower(strings.TrimSpace(email))))
}

func (s *PGStore) UpdateUser(ctx context.Context, user *User) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE users SET
			email=$2, password_hash=$3, google_subject=$4, display_name=$5, phone=$6, photo_url=$7,
			workplace=$8, address=$9, employee_number=$10,
			role=$11, fleet_id=$12, vehicle_id=$13, is_active=$14,
			updated_at=$15, last_login_at=$16
		WHERE id=$1
	`,
		user.ID, user.Email, user.PasswordHash, user.GoogleSubject, user.DisplayName,
		user.Phone, user.PhotoURL, user.Workplace, user.Address, user.EmployeeNo,
		user.Role, user.FleetID, user.VehicleID, user.IsActive,
		user.UpdatedAt, nilTime(user.LastLoginAt),
	)
	return err
}

func (s *PGStore) ListUsers(ctx context.Context, filterRole, filterFleet string) ([]User, error) {
	query := `SELECT id, email, '', '', display_name, phone, photo_url, workplace, address, employee_number,
	                  role, fleet_id, vehicle_id, is_active, created_at, updated_at, last_login_at
	           FROM users WHERE 1=1`
	args := []interface{}{}
	idx := 1

	if filterRole != "" {
		query += fmt.Sprintf(" AND role = $%d", idx)
		args = append(args, filterRole)
		idx++
	}
	if filterFleet != "" {
		query += fmt.Sprintf(" AND fleet_id = $%d", idx)
		args = append(args, filterFleet)
		idx++
	}
	query += " ORDER BY created_at DESC"

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		u, err := s.scanUserFromRows(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, *u)
	}
	return users, rows.Err()
}

// scanUser scans a single user row.
func (s *PGStore) scanUser(row pgx.Row) (*User, error) {
	var u User
	var lastLogin *interface{}
	err := row.Scan(
		&u.ID, &u.Email, &u.PasswordHash, &u.GoogleSubject, &u.DisplayName,
		&u.Phone, &u.PhotoURL, &u.Workplace, &u.Address, &u.EmployeeNo, &u.Role,
		&u.FleetID, &u.VehicleID, &u.IsActive,
		&u.CreatedAt, &u.UpdatedAt, &lastLogin,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("user not found")
		}
		return nil, fmt.Errorf("scan user: %w", err)
	}
	return &u, nil
}

func (s *PGStore) scanUserFromRows(rows pgx.Rows) (*User, error) {
	var u User
	var lastLogin *interface{}
	err := rows.Scan(
		&u.ID, &u.Email, &u.PasswordHash, &u.GoogleSubject, &u.DisplayName,
		&u.Phone, &u.PhotoURL, &u.Workplace, &u.Address, &u.EmployeeNo, &u.Role,
		&u.FleetID, &u.VehicleID, &u.IsActive,
		&u.CreatedAt, &u.UpdatedAt, &lastLogin,
	)
	if err != nil {
		return nil, fmt.Errorf("scan user: %w", err)
	}
	return &u, nil
}

func nilTime(t interface{}) interface{} {
	// If it's a zero time, store NULL
	return t
}

func nilTenantTime(t any) any {
	return t
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "duplicate key") || strings.Contains(msg, "unique constraint")
}
