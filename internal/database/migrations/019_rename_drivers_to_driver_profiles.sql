-- Driver records are fleet-approved operational profiles, not login identity.

DO $$
BEGIN
    IF to_regclass('public.driver_profiles') IS NULL
       AND to_regclass('public.drivers') IS NOT NULL THEN
        ALTER TABLE drivers RENAME TO driver_profiles;
    END IF;
END $$;

ALTER INDEX IF EXISTS idx_drivers_fleet_id RENAME TO idx_driver_profiles_fleet_id;
ALTER INDEX IF EXISTS idx_drivers_user_id RENAME TO idx_driver_profiles_user_id;

CREATE INDEX IF NOT EXISTS idx_driver_profiles_fleet_id
    ON driver_profiles (fleet_id);

CREATE INDEX IF NOT EXISTS idx_driver_profiles_user_id
    ON driver_profiles (user_id) WHERE user_id != '';
