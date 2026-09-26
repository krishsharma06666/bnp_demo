-- Handy checks after a run. Owner: Person 2.
-- docker exec -i postgres psql -U app -d booking_db < db/checks.sql

SELECT 'inserted' AS what, count(*) FROM bookings
UNION ALL
SELECT 'rejected', count(*) FROM rejected_records;

SELECT error_code, count(*) FROM rejected_records GROUP BY 1 ORDER BY 2 DESC;

SELECT booking_status, count(*) FROM bookings GROUP BY 1 ORDER BY 2 DESC;

-- no duplicates may exist
SELECT idempotency_key, count(*) FROM bookings GROUP BY 1 HAVING count(*) > 1;
