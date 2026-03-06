-- Keep driver identity columns consistent for legacy rows.
UPDATE drivers
SET user_id = id
WHERE COALESCE(BTRIM(user_id), '') = ''
  AND COALESCE(BTRIM(id), '') <> '';

-- Align vehicle_id with the canonical assigned_vehicle_ids field.
UPDATE drivers
SET vehicle_id = COALESCE(NULLIF(BTRIM(assigned_vehicle_ids[1]), ''), '')
WHERE COALESCE(BTRIM(vehicle_id), '') IS DISTINCT FROM
      COALESCE(NULLIF(BTRIM(assigned_vehicle_ids[1]), ''), '');

-- Backfill assignment arrays from vehicles table when a row is still empty.
WITH latest_vehicle AS (
  SELECT DISTINCT ON (fleet_id, driver_id)
    fleet_id,
    driver_id,
    id AS vehicle_id
  FROM vehicles
  WHERE COALESCE(BTRIM(driver_id), '') <> ''
  ORDER BY fleet_id, driver_id, last_updated DESC
)
UPDATE drivers d
SET assigned_vehicle_ids = ARRAY[l.vehicle_id],
    vehicle_id = l.vehicle_id
FROM latest_vehicle l
WHERE d.fleet_id = l.fleet_id
  AND d.id = l.driver_id
  AND (
    array_length(d.assigned_vehicle_ids, 1) IS NULL
    OR array_length(d.assigned_vehicle_ids, 1) = 0
  );
