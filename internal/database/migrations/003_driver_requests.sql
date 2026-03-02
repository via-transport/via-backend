-- Via Backend: Driver access requests

CREATE TABLE IF NOT EXISTS driver_requests (
    id         TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL,
    fleet_id   TEXT NOT NULL,
    full_name  TEXT NOT NULL DEFAULT '',
    email      TEXT NOT NULL DEFAULT '',
    phone      TEXT NOT NULL DEFAULT '',
    note       TEXT NOT NULL DEFAULT '',
    status     TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_driver_requests_fleet_status ON driver_requests (fleet_id, status);
CREATE INDEX IF NOT EXISTS idx_driver_requests_user_id ON driver_requests (user_id);
