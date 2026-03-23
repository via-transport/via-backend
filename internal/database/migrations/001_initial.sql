-- Via Backend: Initial Schema
-- PostgreSQL 16
-- All timestamps are UTC (timestamptz).
-- UUIDs are generated application-side (google/uuid) and stored as TEXT for simplicity
-- with NATS-era compatibility.

-- =========================================================================
-- 1. USERS
-- =========================================================================
CREATE TABLE IF NOT EXISTS users (
    id            TEXT PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL DEFAULT '',
    google_subject TEXT NOT NULL DEFAULT '',
    display_name  TEXT NOT NULL DEFAULT '',
    phone         TEXT NOT NULL DEFAULT '',
    photo_url     TEXT NOT NULL DEFAULT '',
    role          TEXT NOT NULL DEFAULT 'passenger',  -- owner | admin | driver | passenger
    fleet_id      TEXT NOT NULL DEFAULT '',
    vehicle_id    TEXT NOT NULL DEFAULT '',
    is_active     BOOLEAN NOT NULL DEFAULT true,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_users_email    ON users (email);
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_google_subject
    ON users (google_subject) WHERE google_subject != '';
CREATE INDEX IF NOT EXISTS idx_users_role     ON users (role);
CREATE INDEX IF NOT EXISTS idx_users_fleet_id ON users (fleet_id) WHERE fleet_id != '';

-- =========================================================================
-- 2. VEHICLES
-- =========================================================================
CREATE TABLE IF NOT EXISTS vehicles (
    id                  TEXT PRIMARY KEY,
    registration_number TEXT NOT NULL DEFAULT '',
    type                TEXT NOT NULL DEFAULT '',
    service_type        TEXT NOT NULL DEFAULT '',
    is_active           BOOLEAN NOT NULL DEFAULT false,
    status              TEXT NOT NULL DEFAULT '',
    status_message      TEXT NOT NULL DEFAULT '',
    current_route_id    TEXT NOT NULL DEFAULT '',
    driver_id           TEXT NOT NULL DEFAULT '',
    driver_name         TEXT NOT NULL DEFAULT '',
    driver_phone        TEXT NOT NULL DEFAULT '',
    fleet_id            TEXT NOT NULL DEFAULT '',
    capacity            INT  NOT NULL DEFAULT 0,
    current_passengers  INT  NOT NULL DEFAULT 0,
    -- location (embedded)
    loc_latitude        DOUBLE PRECISION NOT NULL DEFAULT 0,
    loc_longitude       DOUBLE PRECISION NOT NULL DEFAULT 0,
    loc_heading         DOUBLE PRECISION NOT NULL DEFAULT 0,
    loc_speed           DOUBLE PRECISION NOT NULL DEFAULT 0,
    loc_accuracy        DOUBLE PRECISION NOT NULL DEFAULT 0,
    loc_timestamp       TIMESTAMPTZ,
    last_updated        TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_vehicles_fleet_id  ON vehicles (fleet_id);
CREATE INDEX IF NOT EXISTS idx_vehicles_driver_id  ON vehicles (driver_id) WHERE driver_id != '';
CREATE INDEX IF NOT EXISTS idx_vehicles_is_active  ON vehicles (fleet_id, is_active);

-- =========================================================================
-- 3. DRIVERS
-- =========================================================================
CREATE TABLE IF NOT EXISTS drivers (
    id                  TEXT PRIMARY KEY,
    email               TEXT NOT NULL DEFAULT '',
    full_name           TEXT NOT NULL DEFAULT '',
    phone               TEXT NOT NULL DEFAULT '',
    fleet_id            TEXT NOT NULL DEFAULT '',
    user_id             TEXT NOT NULL DEFAULT '',
    vehicle_id          TEXT NOT NULL DEFAULT '',
    license_number      TEXT NOT NULL DEFAULT '',
    assigned_vehicle_ids TEXT[] NOT NULL DEFAULT '{}',
    is_active           BOOLEAN NOT NULL DEFAULT false,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_drivers_fleet_id ON drivers (fleet_id);
CREATE INDEX IF NOT EXISTS idx_drivers_user_id  ON drivers (user_id) WHERE user_id != '';

-- =========================================================================
-- 4. SPECIAL EVENTS
-- =========================================================================
CREATE TABLE IF NOT EXISTS special_events (
    id            TEXT PRIMARY KEY,
    type          TEXT NOT NULL DEFAULT '',
    vehicle_id    TEXT NOT NULL DEFAULT '',
    driver_id     TEXT NOT NULL DEFAULT '',
    fleet_id      TEXT NOT NULL DEFAULT '',
    timestamp     TIMESTAMPTZ NOT NULL DEFAULT now(),
    message       TEXT NOT NULL DEFAULT '',
    delay_minutes INT  NOT NULL DEFAULT 0,
    location      TEXT NOT NULL DEFAULT '',
    metadata      JSONB
);

CREATE INDEX IF NOT EXISTS idx_events_fleet_id    ON special_events (fleet_id);
CREATE INDEX IF NOT EXISTS idx_events_vehicle_id  ON special_events (vehicle_id) WHERE vehicle_id != '';
CREATE INDEX IF NOT EXISTS idx_events_timestamp   ON special_events (timestamp DESC);

-- =========================================================================
-- 5. DRIVER NOTICES
-- =========================================================================
CREATE TABLE IF NOT EXISTS driver_notices (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL DEFAULT '',
    message     TEXT NOT NULL DEFAULT '',
    vehicle_id  TEXT NOT NULL DEFAULT '',
    driver_id   TEXT NOT NULL DEFAULT '',
    fleet_id    TEXT NOT NULL DEFAULT '',
    route_id    TEXT NOT NULL DEFAULT '',
    priority    TEXT NOT NULL DEFAULT 'low',  -- low | medium | high | urgent
    is_read     BOOLEAN NOT NULL DEFAULT false,
    action_url  TEXT NOT NULL DEFAULT '',
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT now(),
    read_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_notices_fleet_id   ON driver_notices (fleet_id);
CREATE INDEX IF NOT EXISTS idx_notices_vehicle_id ON driver_notices (vehicle_id) WHERE vehicle_id != '';
CREATE INDEX IF NOT EXISTS idx_notices_driver_id  ON driver_notices (driver_id) WHERE driver_id != '';

-- =========================================================================
-- 6. NOTIFICATIONS
-- =========================================================================
CREATE TABLE IF NOT EXISTS notifications (
    id         TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL,
    fleet_id   TEXT NOT NULL DEFAULT '',
    vehicle_id TEXT NOT NULL DEFAULT '',
    type       TEXT NOT NULL DEFAULT '',
    title      TEXT NOT NULL DEFAULT '',
    body       TEXT NOT NULL DEFAULT '',
    data       JSONB,
    is_read    BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    read_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_notif_user_id         ON notifications (user_id);
CREATE INDEX IF NOT EXISTS idx_notif_user_unread      ON notifications (user_id) WHERE NOT is_read;
CREATE INDEX IF NOT EXISTS idx_notif_user_created     ON notifications (user_id, created_at DESC);

-- =========================================================================
-- 7. SUBSCRIPTIONS
-- =========================================================================
CREATE TABLE IF NOT EXISTS subscriptions (
    id          TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL,
    vehicle_id  TEXT NOT NULL,
    fleet_id    TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'active',  -- active | paused | cancelled
    -- preferences (embedded)
    pref_notify_arrival BOOLEAN NOT NULL DEFAULT true,
    pref_notify_delay   BOOLEAN NOT NULL DEFAULT true,
    pref_notify_event   BOOLEAN NOT NULL DEFAULT true,
    pref_radius_meters  INT     NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sub_user_id    ON subscriptions (user_id);
CREATE INDEX IF NOT EXISTS idx_sub_vehicle_id ON subscriptions (vehicle_id) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_sub_fleet_id   ON subscriptions (fleet_id);
