-- Passenger identity remains in users. This profile table gives passengers a
-- domain-specific row that can grow independently from login/account data.

CREATE TABLE IF NOT EXISTS passenger_profiles (
    user_id              TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    fleet_id             TEXT NOT NULL DEFAULT '',
    preferred_vehicle_id TEXT NOT NULL DEFAULT '',
    is_active            BOOLEAN NOT NULL DEFAULT true,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_passenger_profiles_fleet_id
    ON passenger_profiles (fleet_id) WHERE fleet_id != '';

INSERT INTO passenger_profiles (user_id, fleet_id, is_active, created_at, updated_at)
SELECT id, fleet_id, is_active, created_at, updated_at
FROM users
WHERE role = 'passenger'
ON CONFLICT (user_id) DO UPDATE SET
    fleet_id = EXCLUDED.fleet_id,
    is_active = EXCLUDED.is_active,
    updated_at = EXCLUDED.updated_at;

CREATE OR REPLACE FUNCTION sync_passenger_profile_for_user()
RETURNS trigger AS $$
BEGIN
    IF NEW.role = 'passenger' THEN
        INSERT INTO passenger_profiles (user_id, fleet_id, is_active, created_at, updated_at)
        VALUES (NEW.id, NEW.fleet_id, NEW.is_active, NEW.created_at, NEW.updated_at)
        ON CONFLICT (user_id) DO UPDATE SET
            fleet_id = EXCLUDED.fleet_id,
            is_active = EXCLUDED.is_active,
            updated_at = EXCLUDED.updated_at;
    ELSE
        DELETE FROM passenger_profiles WHERE user_id = NEW.id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_passenger_profile_for_user ON users;
CREATE TRIGGER trg_sync_passenger_profile_for_user
AFTER INSERT OR UPDATE OF role, fleet_id, is_active, updated_at ON users
FOR EACH ROW
EXECUTE FUNCTION sync_passenger_profile_for_user();
