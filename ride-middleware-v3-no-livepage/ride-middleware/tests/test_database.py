"""Database (Person 2): migrations are safe to re-run and to run concurrently; Supabase REST API is locked."""
import threading

import psycopg

from db.migrate import migrate


def test_rls_on_every_table_but_owner_still_works(db, pg_url):
    """Supabase REST API (anon role) sees nothing; our services (table owner) work normally."""
    tables = [r[0] for r in db.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")]
    no_rls = [r[0] for r in db.execute("SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                                       "WHERE n.nspname='public' AND c.relkind='r' AND NOT c.relrowsecurity")]
    assert tables and no_rls == []
    db.execute("CREATE ROLE anon_test NOLOGIN")
    db.execute("GRANT USAGE ON SCHEMA public TO anon_test; GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon_test")
    db.commit()
    with psycopg.connect(pg_url) as c:
        assert c.execute("SELECT count(*) FROM drivers").fetchone()[0] == 500          # owner: full access
        c.execute("SET ROLE anon_test")
        assert c.execute("SELECT count(*) FROM drivers").fetchone()[0] == 0            # public API role: nothing
        c.rollback()
    db.execute("DROP OWNED BY anon_test; DROP ROLE anon_test")
    db.commit()


def test_migrations_rerun_is_a_no_op_and_concurrent_runners_apply_once(db, pg_url):
    assert migrate(pg_url, verbose=False) == []                        # db fixture already applied all
    db.execute("DROP TABLE schema_migrations")                         # pretend nothing is recorded
    db.commit()
    results = []
    ts = [threading.Thread(target=lambda: results.append(migrate(pg_url, verbose=False))) for _ in range(4)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    applied = [f for r in results for f in r]
    assert sorted(applied) == sorted(set(applied)) and len(applied) == 5      # every file applied exactly once
    assert db.execute("SELECT count(*) FROM drivers").fetchone()[0] == 500   # seeds not duplicated
    assert db.execute("SELECT count(*) FROM schema_migrations").fetchone()[0] == 5


def test_constraints_protect_new_rows(db):
    import pytest
    for bad in ["INSERT INTO bookings (booking_id, user_id, pickup_location, destination_location, booking_status, "
                "fare, idempotency_key) VALUES (1, 1, 'Hebbal', 'Hebbal', 'REQUESTED', 100, 'X1')",     # same place
                "INSERT INTO bookings (booking_id, user_id, pickup_location, destination_location, booking_status, "
                "fare, idempotency_key) VALUES (2, 1, 'Hebbal', 'Airport', 'FLYING', 100, 'X2')",       # bad status
                "INSERT INTO bookings (booking_id, user_id, pickup_location, destination_location, booking_status, "
                "fare, idempotency_key) VALUES (3, 1, 'Hebbal', 'Airport', 'REQUESTED', -5, 'X3')"]:    # bad fare
        with pytest.raises(psycopg.errors.CheckViolation):
            db.execute(bad)
        db.rollback()
