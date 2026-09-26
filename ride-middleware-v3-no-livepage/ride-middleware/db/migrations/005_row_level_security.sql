-- v3.1 (Person 2, SecOps): lock the tables against Supabase's public REST API.
-- On Supabase every table in "public" is reachable through the auto-generated REST API with the
-- project's anon key. RLS ON + no policies = that API sees nothing. Our services connect as the
-- table OWNER (postgres / app), which bypasses RLS, so the middleware keeps working unchanged.
-- On the local postgres this changes nothing for the app user. Safe to re-run.
ALTER TABLE users            ENABLE ROW LEVEL SECURITY;
ALTER TABLE drivers          ENABLE ROW LEVEL SECURITY;
ALTER TABLE vehicles         ENABLE ROW LEVEL SECURITY;
ALTER TABLE bookings         ENABLE ROW LEVEL SECURITY;
ALTER TABLE payments         ENABLE ROW LEVEL SECURITY;
ALTER TABLE locations        ENABLE ROW LEVEL SECURITY;
ALTER TABLE driver_responses ENABLE ROW LEVEL SECURITY;
ALTER TABLE processed_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE rejected_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE schema_migrations ENABLE ROW LEVEL SECURITY;
