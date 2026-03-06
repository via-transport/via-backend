UPDATE tenants
SET
  vehicle_limit = CASE
    WHEN COALESCE(vehicle_limit, 0) < 10 THEN 10
    ELSE vehicle_limit
  END,
  passenger_limit = CASE
    WHEN COALESCE(passenger_limit, 0) < 250 THEN 250
    ELSE passenger_limit
  END,
  driver_limit = CASE
    WHEN COALESCE(driver_limit, 0) < 10 THEN 10
    ELSE driver_limit
  END,
  event_hourly_limit = CASE
    WHEN COALESCE(event_hourly_limit, 0) < 60 THEN 60
    ELSE event_hourly_limit
  END,
  updated_at = NOW()
WHERE UPPER(COALESCE(plan_type, 'TRIAL')) = 'TRIAL'
  AND (
    COALESCE(vehicle_limit, 0) < 10
    OR COALESCE(passenger_limit, 0) < 250
    OR COALESCE(driver_limit, 0) < 10
    OR COALESCE(event_hourly_limit, 0) < 60
  );
