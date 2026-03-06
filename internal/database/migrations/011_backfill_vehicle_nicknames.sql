UPDATE vehicles
SET nickname = COALESCE(
    NULLIF(BTRIM(registration_number), ''),
    CONCAT('Vehicle ', LEFT(id, 8))
)
WHERE nickname IS NULL OR BTRIM(nickname) = '';
