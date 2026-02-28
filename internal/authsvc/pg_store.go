package authsvc

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

func (s *PGStore) CreateUser(ctx context.Context, user *User) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO users (id, email, password_hash, display_name, phone, photo_url,
		                    role, fleet_id, vehicle_id, is_active, created_at, updated_at, last_login_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
	`,
		user.ID, strings.ToLower(strings.TrimSpace(user.Email)),
		user.PasswordHash, user.DisplayName,
		user.Phone, user.PhotoURL, user.Role,
		user.FleetID, user.VehicleID, user.IsActive,
		user.CreatedAt, user.UpdatedAt, nilTime(user.LastLoginAt),
	)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "unique constraint") {
			return errors.New("email already registered")
		}
		return fmt.Errorf("insert user: %w", err)
	}
	return nil
}

func (s *PGStore) GetUser(ctx context.Context, userID string) (*User, error) {
	return s.scanUser(s.pool.QueryRow(ctx, `
		SELECT id, email, password_hash, display_name, phone, photo_url,
		       role, fleet_id, vehicle_id, is_active, created_at, updated_at, last_login_at
		FROM users WHERE id = $1
	`, userID))
}

func (s *PGStore) GetUserByEmail(ctx context.Context, email string) (*User, error) {
	return s.scanUser(s.pool.QueryRow(ctx, `
		SELECT id, email, password_hash, display_name, phone, photo_url,
		       role, fleet_id, vehicle_id, is_active, created_at, updated_at, last_login_at
		FROM users WHERE email = $1
	`, strings.ToLower(strings.TrimSpace(email))))
}

func (s *PGStore) UpdateUser(ctx context.Context, user *User) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE users SET
			email=$2, password_hash=$3, display_name=$4, phone=$5, photo_url=$6,
			role=$7, fleet_id=$8, vehicle_id=$9, is_active=$10,
			updated_at=$11, last_login_at=$12
		WHERE id=$1
	`,
		user.ID, user.Email, user.PasswordHash, user.DisplayName,
		user.Phone, user.PhotoURL, user.Role,
		user.FleetID, user.VehicleID, user.IsActive,
		user.UpdatedAt, nilTime(user.LastLoginAt),
	)
	return err
}

func (s *PGStore) ListUsers(ctx context.Context, filterRole, filterFleet string) ([]User, error) {
	query := `SELECT id, email, '', display_name, phone, photo_url,
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
		&u.ID, &u.Email, &u.PasswordHash, &u.DisplayName,
		&u.Phone, &u.PhotoURL, &u.Role,
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
		&u.ID, &u.Email, &u.PasswordHash, &u.DisplayName,
		&u.Phone, &u.PhotoURL, &u.Role,
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
