ALTER TABLE driver_requests
ADD COLUMN IF NOT EXISTS request_type TEXT NOT NULL DEFAULT 'vehicle_assignment';

CREATE INDEX IF NOT EXISTS idx_driver_requests_fleet_status_type
ON driver_requests (fleet_id, status, request_type);
