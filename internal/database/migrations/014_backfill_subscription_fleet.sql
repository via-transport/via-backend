-- Backfill legacy pending join requests that were created without fleet scope.
-- We infer fleet_id from the referenced vehicle id or registration number.
UPDATE subscriptions AS s
SET
    fleet_id = v.fleet_id,
    updated_at = NOW()
FROM vehicles AS v
WHERE s.fleet_id = ''
  AND s.status = 'pending'
  AND v.fleet_id <> ''
  AND (
      s.vehicle_id = v.id
      OR LOWER(TRIM(s.vehicle_id)) = LOWER(TRIM(v.registration_number))
      OR LOWER(REGEXP_REPLACE(s.vehicle_id, '[^a-zA-Z0-9]+', '', 'g')) =
         LOWER(REGEXP_REPLACE(v.registration_number, '[^a-zA-Z0-9]+', '', 'g'))
  );
