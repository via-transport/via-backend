ALTER TABLE operations
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

CREATE INDEX IF NOT EXISTS idx_operations_idempotency_key
    ON operations(idempotency_key)
    WHERE idempotency_key IS NOT NULL;
