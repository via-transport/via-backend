package requestsvc

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

func (s *PGStore) Put(ctx context.Context, req *DriverRequest) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO driver_requests (
			id, user_id, fleet_id, request_type, vehicle_id, full_name, email, phone, note, status, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (id) DO UPDATE SET
			user_id=$2, fleet_id=$3, request_type=$4, vehicle_id=$5, full_name=$6, email=$7, phone=$8, note=$9, status=$10, updated_at=$12
	`,
		req.ID, req.UserID, req.FleetID, req.RequestType, req.VehicleID, req.FullName, req.Email,
		req.Phone, req.Note, req.Status, req.CreatedAt, req.UpdatedAt,
	)
	return err
}

func (s *PGStore) Get(ctx context.Context, id string) (*DriverRequest, error) {
	return s.scanOne(s.pool.QueryRow(ctx, `
		SELECT id, user_id, fleet_id, request_type, vehicle_id, full_name, email, phone, note, status, created_at, updated_at
		FROM driver_requests
		WHERE id=$1
	`, id))
}

func (s *PGStore) List(ctx context.Context, fleetID, status, requestType string) ([]DriverRequest, error) {
	query := `
		SELECT id, user_id, fleet_id, request_type, vehicle_id, full_name, email, phone, note, status, created_at, updated_at
		FROM driver_requests`
	args := []any{}
	if fleetID != "" {
		query += ` WHERE fleet_id=$1`
		args = append(args, fleetID)
	}
	if status != "" {
		if len(args) == 0 {
			query += ` WHERE`
		} else {
			query += ` AND`
		}
		query += fmt.Sprintf(` status=$%d`, len(args)+1)
		args = append(args, status)
	}
	if requestType != "" {
		if len(args) == 0 {
			query += ` WHERE`
		} else {
			query += ` AND`
		}
		query += fmt.Sprintf(` request_type=$%d`, len(args)+1)
		args = append(args, requestType)
	}
	query += ` ORDER BY created_at DESC`

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []DriverRequest
	for rows.Next() {
		item, err := scanDriverRequest(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	return items, rows.Err()
}

func (s *PGStore) FindPendingByUser(ctx context.Context, fleetID, userID, requestType string) (*DriverRequest, error) {
	if requestType == "" {
		requestType = RequestTypeVehicleAssignment
	}
	return s.scanOne(s.pool.QueryRow(ctx, `
		SELECT id, user_id, fleet_id, request_type, vehicle_id, full_name, email, phone, note, status, created_at, updated_at
		FROM driver_requests
		WHERE fleet_id=$1 AND user_id=$2 AND status=$3 AND request_type=$4
		ORDER BY created_at DESC
		LIMIT 1
		`, fleetID, userID, StatusPending, requestType))
}

func (s *PGStore) RevokeApprovedVehicleAssignments(ctx context.Context, fleetID, userID string) (int, error) {
	normalizedFleetID := strings.TrimSpace(fleetID)
	normalizedUserID := strings.TrimSpace(userID)
	if normalizedFleetID == "" || normalizedUserID == "" {
		return 0, nil
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE driver_requests
		SET status=$3, updated_at=NOW()
		WHERE fleet_id=$1
		  AND user_id=$2
		  AND request_type=$4
		  AND status=$5
	`,
		normalizedFleetID,
		normalizedUserID,
		StatusRevoked,
		RequestTypeVehicleAssignment,
		StatusApproved,
	)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

type reqScanner interface {
	Scan(dest ...any) error
}

func (s *PGStore) scanOne(row reqScanner) (*DriverRequest, error) {
	req, err := scanDriverRequest(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("driver request not found")
		}
		return nil, err
	}
	return req, nil
}

func scanDriverRequest(row reqScanner) (*DriverRequest, error) {
	var req DriverRequest
	if err := row.Scan(
		&req.ID,
		&req.UserID,
		&req.FleetID,
		&req.RequestType,
		&req.VehicleID,
		&req.FullName,
		&req.Email,
		&req.Phone,
		&req.Note,
		&req.Status,
		&req.CreatedAt,
		&req.UpdatedAt,
	); err != nil {
		return nil, err
	}
	req.RequestType = defaultDriverRequestType(req.RequestType, req.VehicleID)
	return &req, nil
}
