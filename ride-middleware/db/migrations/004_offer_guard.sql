-- v3.1 (Person 2): an answer from a driver who was NEVER offered the ride is stored for audit
-- with outcome NOT_OFFERED, and can never win the ride. Additive and safe to re-run.
ALTER TABLE driver_responses DROP CONSTRAINT IF EXISTS driver_responses_outcome_check;
ALTER TABLE driver_responses ADD CONSTRAINT driver_responses_outcome_check
    CHECK (outcome IN ('ASSIGNED', 'LOST', 'DECLINED', 'EXPIRED', 'NOT_OFFERED'));
