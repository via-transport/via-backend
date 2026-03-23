ALTER TABLE users
    ADD COLUMN IF NOT EXISTS google_subject TEXT NOT NULL DEFAULT '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_google_subject
    ON users (google_subject) WHERE google_subject != '';
