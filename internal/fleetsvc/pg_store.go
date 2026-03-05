package fleetsvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGStore implements FleetStore using PostgreSQL.
type PGStore struct {
	pool *pgxpool.Pool
}

// NewPGStore creates a fleet store backed by PostgreSQL.
func NewPGStore(pool *pgxpool.Pool) *PGStore {
	return &PGStore{pool: pool}
}

// Compile-time interface check.
var _ FleetStore = (*PGStore)(nil)
var _ VehicleLimitEnforcer = (*PGStore)(nil)

// -------------------------------------------------------------------------
// Vehicles
// -------------------------------------------------------------------------

func (s *PGStore) PutVehicle(ctx context.Context, v *Vehicle) error {
	return s.putVehicle(ctx, s.pool, v)
}

func (s *PGStore) CreateVehicleIfWithinLimit(ctx context.Context, v *Vehicle, vehicleLimit int) error {
	if vehicleLimit <= 0 {
		return s.putVehicle(ctx, s.pool, v)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var tenantID string
	if err := tx.QueryRow(ctx, `SELECT id FROM tenants WHERE id=$1 FOR UPDATE`, v.FleetID).Scan(&tenantID); err != nil {
		return err
	}

	var currentCount int
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM vehicles WHERE fleet_id=$1`, v.FleetID).Scan(&currentCount); err != nil {
		return err
	}
	if currentCount >= vehicleLimit {
		return ErrVehicleLimitReached
	}

	if err := s.putVehicle(ctx, tx, v); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

type vehicleExec interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func (s *PGStore) putVehicle(ctx context.Context, exec vehicleExec, v *Vehicle) error {
	var locTs interface{} = nil
	if v.CurrentLocation != nil && !v.CurrentLocation.Timestamp.IsZero() {
		locTs = v.CurrentLocation.Timestamp
	}
	locLat, locLng, locHead, locSpd, locAcc := 0.0, 0.0, 0.0, 0.0, 0.0
	if v.CurrentLocation != nil {
		locLat = v.CurrentLocation.Latitude
		locLng = v.CurrentLocation.Longitude
		locHead = v.CurrentLocation.Heading
		locSpd = v.CurrentLocation.Speed
		locAcc = v.CurrentLocation.Accuracy
	}

	_, err := exec.Exec(ctx, `
		INSERT INTO vehicles (id, registration_number, nickname, type, service_type, is_active, status,
		  status_message, current_route_id, driver_id, driver_name, driver_phone, fleet_id,
		  capacity, current_passengers, loc_latitude, loc_longitude, loc_heading, loc_speed,
		  loc_accuracy, loc_timestamp, last_updated, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
		ON CONFLICT (id) DO UPDATE SET
		  registration_number=$2, nickname=$3, type=$4, service_type=$5, is_active=$6, status=$7,
		  status_message=$8, current_route_id=$9, driver_id=$10, driver_name=$11, driver_phone=$12,
		  fleet_id=$13, capacity=$14, current_passengers=$15,
		  loc_latitude=$16, loc_longitude=$17, loc_heading=$18, loc_speed=$19,
		  loc_accuracy=$20, loc_timestamp=$21, last_updated=$22
	`,
		v.ID, v.RegistrationNumber, v.Nickname, v.Type, v.ServiceType, v.IsActive, v.Status,
		v.StatusMessage, v.CurrentRouteID, v.DriverID, v.DriverName, v.DriverPhone, v.FleetID,
		v.Capacity, v.CurrentPassengers,
		locLat, locLng, locHead, locSpd, locAcc, locTs,
		v.LastUpdated, v.CreatedAt,
	)
	return err
}

func (s *PGStore) GetVehicle(ctx context.Context, fleetID, vehicleID string) (*Vehicle, error) {
	return s.scanVehicle(s.pool.QueryRow(ctx, vehicleSelectSQL+" WHERE id=$1 AND fleet_id=$2", vehicleID, fleetID))
}

func (s *PGStore) GetVehicleByID(ctx context.Context, vehicleID string) (*Vehicle, error) {
	return s.scanVehicle(s.pool.QueryRow(ctx, vehicleSelectSQL+" WHERE id=$1", vehicleID))
}

func (s *PGStore) DeleteVehicle(ctx context.Context, fleetID, vehicleID string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM vehicles WHERE id=$1 AND fleet_id=$2`, vehicleID, fleetID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("vehicle not found")
	}
	return nil
}

func (s *PGStore) ListVehicles(ctx context.Context, fleetID string) ([]Vehicle, error) {
	query := vehicleSelectSQL
	args := []interface{}{}
	if fleetID != "" {
		query += " WHERE fleet_id=$1"
		args = append(args, fleetID)
	}
	query += " ORDER BY last_updated DESC"
	return s.queryVehicles(ctx, query, args...)
}

func (s *PGStore) ListVehiclesForDriver(ctx context.Context, fleetID, driverID string) ([]Vehicle, error) {
	if strings.TrimSpace(driverID) == "" {
		return []Vehicle{}, nil
	}
	query := vehicleSelectSQL + " WHERE fleet_id=$1 AND driver_id=$2 ORDER BY last_updated DESC"
	return s.queryVehicles(ctx, query, fleetID, driverID)
}

const vehicleSelectSQL = `SELECT id, registration_number, nickname, type, service_type, is_active, status,
	status_message, current_route_id, driver_id, driver_name, driver_phone, fleet_id,
	capacity, current_passengers, loc_latitude, loc_longitude, loc_heading, loc_speed,
	loc_accuracy, loc_timestamp, last_updated, created_at
	FROM vehicles`

func (s *PGStore) scanVehicle(row pgx.Row) (*Vehicle, error) {
	var v Vehicle
	var locLat, locLng, locHead, locSpd, locAcc float64
	var locTs *time.Time
	err := row.Scan(
		&v.ID, &v.RegistrationNumber, &v.Nickname, &v.Type, &v.ServiceType, &v.IsActive, &v.Status,
		&v.StatusMessage, &v.CurrentRouteID, &v.DriverID, &v.DriverName, &v.DriverPhone,
		&v.FleetID, &v.Capacity, &v.CurrentPassengers,
		&locLat, &locLng, &locHead, &locSpd, &locAcc, &locTs,
		&v.LastUpdated, &v.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("vehicle not found")
		}
		return nil, fmt.Errorf("scan vehicle: %w", err)
	}
	if locLat != 0 || locLng != 0 {
		v.CurrentLocation = &VehicleLocation{
			Latitude: locLat, Longitude: locLng,
			Heading: locHead, Speed: locSpd, Accuracy: locAcc,
		}
		if locTs != nil {
			v.CurrentLocation.Timestamp = *locTs
		}
	}
	return &v, nil
}

func (s *PGStore) queryVehicles(ctx context.Context, query string, args ...interface{}) ([]Vehicle, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Vehicle
	for rows.Next() {
		var v Vehicle
		var locLat, locLng, locHead, locSpd, locAcc float64
		var locTs *time.Time
		if err := rows.Scan(
			&v.ID, &v.RegistrationNumber, &v.Nickname, &v.Type, &v.ServiceType, &v.IsActive, &v.Status,
			&v.StatusMessage, &v.CurrentRouteID, &v.DriverID, &v.DriverName, &v.DriverPhone,
			&v.FleetID, &v.Capacity, &v.CurrentPassengers,
			&locLat, &locLng, &locHead, &locSpd, &locAcc, &locTs,
			&v.LastUpdated, &v.CreatedAt,
		); err != nil {
			return nil, err
		}
		if locLat != 0 || locLng != 0 {
			v.CurrentLocation = &VehicleLocation{
				Latitude: locLat, Longitude: locLng,
				Heading: locHead, Speed: locSpd, Accuracy: locAcc,
			}
			if locTs != nil {
				v.CurrentLocation.Timestamp = *locTs
			}
		}
		result = append(result, v)
	}
	return result, rows.Err()
}

// -------------------------------------------------------------------------
// Drivers
// -------------------------------------------------------------------------

func (s *PGStore) PutDriver(ctx context.Context, d *Driver) error {
	vehicleIDs := d.AssignedVehicleIDs
	if vehicleIDs == nil {
		vehicleIDs = []string{}
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO drivers (id, email, full_name, phone, fleet_id,
		  assigned_vehicle_ids, is_active, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (id) DO UPDATE SET
		  email=$2, full_name=$3, phone=$4, fleet_id=$5,
		  assigned_vehicle_ids=$6, is_active=$7, updated_at=$9
	`,
		d.ID, d.Email, d.FullName, d.Phone, d.FleetID,
		vehicleIDs, d.IsActive, d.CreatedAt, d.UpdatedAt,
	)
	return err
}

func (s *PGStore) GetDriver(ctx context.Context, fleetID, driverID string) (*Driver, error) {
	var d Driver
	err := s.pool.QueryRow(ctx, `
		SELECT id, email, full_name, phone, fleet_id,
		       assigned_vehicle_ids, is_active, created_at, updated_at
		FROM drivers WHERE id=$1 AND fleet_id=$2
	`, driverID, fleetID).Scan(
		&d.ID, &d.Email, &d.FullName, &d.Phone, &d.FleetID,
		&d.AssignedVehicleIDs, &d.IsActive, &d.CreatedAt, &d.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("driver not found")
		}
		return nil, err
	}
	return &d, nil
}

func (s *PGStore) DeleteDriver(ctx context.Context, fleetID, driverID string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM drivers WHERE id=$1 AND fleet_id=$2`, driverID, fleetID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("driver not found")
	}
	return nil
}

func (s *PGStore) ListDrivers(ctx context.Context, fleetID string) ([]Driver, error) {
	query := `SELECT id, email, full_name, phone, fleet_id,
	                  assigned_vehicle_ids, is_active, created_at, updated_at
	           FROM drivers`
	args := []interface{}{}
	if fleetID != "" {
		query += " WHERE fleet_id=$1"
		args = append(args, fleetID)
	}
	query += " ORDER BY created_at DESC"

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Driver
	for rows.Next() {
		var d Driver
		if err := rows.Scan(
			&d.ID, &d.Email, &d.FullName, &d.Phone, &d.FleetID,
			&d.AssignedVehicleIDs, &d.IsActive, &d.CreatedAt, &d.UpdatedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, d)
	}
	return result, rows.Err()
}

// -------------------------------------------------------------------------
// Events
// -------------------------------------------------------------------------

func (s *PGStore) PutEvent(ctx context.Context, e *SpecialEvent) error {
	metaJSON, _ := json.Marshal(e.Metadata)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO special_events (id, type, vehicle_id, driver_id, fleet_id,
		  timestamp, message, delay_minutes, location, metadata)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (id) DO UPDATE SET
		  type=$2, vehicle_id=$3, driver_id=$4, fleet_id=$5,
		  timestamp=$6, message=$7, delay_minutes=$8, location=$9, metadata=$10
	`,
		e.ID, e.Type, e.VehicleID, e.DriverID, e.FleetID,
		e.Timestamp, e.Message, e.DelayMinutes, e.Location, metaJSON,
	)
	return err
}

func (s *PGStore) GetEvent(ctx context.Context, eventID string) (*SpecialEvent, error) {
	var e SpecialEvent
	var metaJSON []byte
	err := s.pool.QueryRow(ctx, `
		SELECT id, type, vehicle_id, driver_id, fleet_id,
		       timestamp, message, delay_minutes, location, metadata
		FROM special_events WHERE id=$1
	`, eventID).Scan(
		&e.ID, &e.Type, &e.VehicleID, &e.DriverID, &e.FleetID,
		&e.Timestamp, &e.Message, &e.DelayMinutes, &e.Location, &metaJSON,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("event not found")
		}
		return nil, err
	}
	if len(metaJSON) > 0 {
		_ = json.Unmarshal(metaJSON, &e.Metadata)
	}
	return &e, nil
}

func (s *PGStore) ListEvents(ctx context.Context, fleetID, vehicleID string) ([]SpecialEvent, error) {
	query := `SELECT id, type, vehicle_id, driver_id, fleet_id,
	                  timestamp, message, delay_minutes, location, metadata
	           FROM special_events WHERE 1=1`
	args := []interface{}{}
	idx := 1
	if fleetID != "" {
		query += fmt.Sprintf(" AND fleet_id=$%d", idx)
		args = append(args, fleetID)
		idx++
	}
	if vehicleID != "" {
		query += fmt.Sprintf(" AND vehicle_id=$%d", idx)
		args = append(args, vehicleID)
		idx++
	}
	query += " ORDER BY timestamp DESC"

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []SpecialEvent
	for rows.Next() {
		var e SpecialEvent
		var metaJSON []byte
		if err := rows.Scan(
			&e.ID, &e.Type, &e.VehicleID, &e.DriverID, &e.FleetID,
			&e.Timestamp, &e.Message, &e.DelayMinutes, &e.Location, &metaJSON,
		); err != nil {
			return nil, err
		}
		if len(metaJSON) > 0 {
			_ = json.Unmarshal(metaJSON, &e.Metadata)
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

// -------------------------------------------------------------------------
// Notices
// -------------------------------------------------------------------------

func (s *PGStore) PutNotice(ctx context.Context, n *DriverNotice) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO driver_notices (id, title, message, vehicle_id, driver_id, fleet_id,
		  route_id, priority, is_read, action_url, timestamp, read_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (id) DO UPDATE SET
		  title=$2, message=$3, vehicle_id=$4, driver_id=$5, fleet_id=$6,
		  route_id=$7, priority=$8, is_read=$9, action_url=$10, timestamp=$11, read_at=$12
	`,
		n.ID, n.Title, n.Message, n.VehicleID, n.DriverID, n.FleetID,
		n.RouteID, n.Priority, n.IsRead, n.ActionURL, n.Timestamp, nilTime(n.ReadAt),
	)
	return err
}

func (s *PGStore) GetNotice(ctx context.Context, noticeID string) (*DriverNotice, error) {
	var n DriverNotice
	var readAt *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT id, title, message, vehicle_id, driver_id, fleet_id,
		       route_id, priority, is_read, action_url, timestamp, read_at
		FROM driver_notices WHERE id=$1
	`, noticeID).Scan(
		&n.ID, &n.Title, &n.Message, &n.VehicleID, &n.DriverID, &n.FleetID,
		&n.RouteID, &n.Priority, &n.IsRead, &n.ActionURL, &n.Timestamp, &readAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("notice not found")
		}
		return nil, err
	}
	if readAt != nil {
		n.ReadAt = *readAt
	}
	return &n, nil
}

func (s *PGStore) ListNotices(ctx context.Context, fleetID, vehicleID, driverID string) ([]DriverNotice, error) {
	query := `SELECT id, title, message, vehicle_id, driver_id, fleet_id,
	                  route_id, priority, is_read, action_url, timestamp, read_at
	           FROM driver_notices WHERE 1=1`
	args := []interface{}{}
	idx := 1
	if fleetID != "" {
		query += fmt.Sprintf(" AND fleet_id=$%d", idx)
		args = append(args, fleetID)
		idx++
	}
	if vehicleID != "" {
		query += fmt.Sprintf(" AND vehicle_id=$%d", idx)
		args = append(args, vehicleID)
		idx++
	}
	if driverID != "" {
		query += fmt.Sprintf(" AND driver_id=$%d", idx)
		args = append(args, driverID)
		idx++
	}
	query += " ORDER BY timestamp DESC"

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []DriverNotice
	for rows.Next() {
		var n DriverNotice
		var readAt *time.Time
		if err := rows.Scan(
			&n.ID, &n.Title, &n.Message, &n.VehicleID, &n.DriverID, &n.FleetID,
			&n.RouteID, &n.Priority, &n.IsRead, &n.ActionURL, &n.Timestamp, &readAt,
		); err != nil {
			return nil, err
		}
		if readAt != nil {
			n.ReadAt = *readAt
		}
		result = append(result, n)
	}
	return result, rows.Err()
}

func nilTime(t interface{}) interface{} {
	return t
}
