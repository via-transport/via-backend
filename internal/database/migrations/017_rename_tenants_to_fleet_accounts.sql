-- Rename the SaaS-oriented tenant tables to fleet account terminology.
-- Existing API/code may still use "tenant" internally, but the database now
-- names the customer/account boundary explicitly.

DO $$
BEGIN
    IF to_regclass('public.fleet_accounts') IS NULL
       AND to_regclass('public.tenants') IS NOT NULL THEN
        ALTER TABLE tenants RENAME TO fleet_accounts;
    END IF;
END $$;

ALTER INDEX IF EXISTS idx_tenants_status RENAME TO idx_fleet_accounts_status;

DO $$
BEGIN
    IF to_regclass('public.fleet_memberships') IS NULL
       AND to_regclass('public.tenant_memberships') IS NOT NULL THEN
        ALTER TABLE tenant_memberships RENAME TO fleet_memberships;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'fleet_memberships'
          AND column_name = 'tenant_id'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'fleet_memberships'
          AND column_name = 'fleet_account_id'
    ) THEN
        ALTER TABLE fleet_memberships RENAME COLUMN tenant_id TO fleet_account_id;
    END IF;
END $$;

ALTER INDEX IF EXISTS idx_tenant_memberships_tenant_id
    RENAME TO idx_fleet_memberships_fleet_account_id;

CREATE INDEX IF NOT EXISTS idx_fleet_memberships_fleet_account_id
    ON fleet_memberships (fleet_account_id);

INSERT INTO fleet_memberships (user_id, fleet_account_id, role, created_at, updated_at)
SELECT u.id, u.fleet_id, u.role, u.created_at, u.updated_at
FROM users u
JOIN fleet_accounts fa ON fa.id = u.fleet_id
WHERE u.fleet_id != ''
ON CONFLICT (user_id, fleet_account_id) DO UPDATE SET
    role = EXCLUDED.role,
    updated_at = EXCLUDED.updated_at;
