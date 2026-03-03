ALTER TABLE operations
ADD COLUMN IF NOT EXISTS fleet_id TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_operations_fleet_created_at
ON operations (fleet_id, created_at DESC);
