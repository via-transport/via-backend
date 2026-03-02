-- Via Backend: Tenants / billing / plan state

CREATE TABLE IF NOT EXISTS tenants (
    id                               TEXT PRIMARY KEY,
    name                             TEXT NOT NULL DEFAULT '',
    plan_type                        TEXT NOT NULL DEFAULT 'TRIAL',
    subscription_status              TEXT NOT NULL DEFAULT 'TRIAL',
    trial_started_at                 TIMESTAMPTZ,
    trial_ends_at                    TIMESTAMPTZ,
    grace_ends_at                    TIMESTAMPTZ,
    vehicle_limit                    INT NOT NULL DEFAULT 2,
    passenger_limit                  INT NOT NULL DEFAULT 100,
    driver_limit                     INT NOT NULL DEFAULT 2,
    location_publish_interval_seconds INT NOT NULL DEFAULT 3,
    event_hourly_limit               INT NOT NULL DEFAULT 30,
    created_at                       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tenants_status ON tenants (subscription_status);
