"""Shared test fixtures.

DB tests need a real PostgreSQL. They use, in this order:
  1. TEST_DATABASE_URL  (docker: postgresql://app:app@localhost:5432/ridehail_test  - it gets WIPED)
  2. an embedded Postgres from the `pgserver` package (pip install pgserver)
  3. otherwise DB tests are skipped
Every `db` test starts from an empty schema with all migrations (team schema + middleware + seed).
"""
import os
import tempfile

import pytest

from db.migrate import migrate


@pytest.fixture(scope="session")
def pg_url():
    url = os.getenv("TEST_DATABASE_URL")
    if url:
        if "test" not in url.rsplit("/", 1)[-1].split("?")[0]:
            pytest.exit("TEST_DATABASE_URL must point to a *test* database - tests wipe it")
        return url
    try:
        import pgserver
    except ImportError:
        pytest.skip("no TEST_DATABASE_URL and pgserver not installed")
    srv = pgserver.get_server(tempfile.mkdtemp(prefix="pgtest_"), cleanup_mode="stop")
    return srv.get_uri()


def fresh_schema(url):
    import psycopg
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    migrate(url, verbose=False)


@pytest.fixture()
def db(pg_url):
    """Empty ridehail schema + seed (1,200 users, 500 drivers, 500 vehicles, 18 locations)."""
    import psycopg
    fresh_schema(pg_url)
    with psycopg.connect(pg_url) as conn:
        yield conn


@pytest.fixture()
def paydb(db):
    """Payments live in the same database now (team schema has FKs payments -> bookings)."""
    return db


@pytest.fixture()
def scratch_db_url(pg_url):
    """A separate throwaway database (for the real-outage test), with the full schema."""
    import psycopg
    from psycopg.conninfo import conninfo_to_dict, make_conninfo
    with psycopg.connect(pg_url, autocommit=True) as c:
        c.execute("DROP DATABASE IF EXISTS outage_test WITH (FORCE)")
        c.execute("CREATE DATABASE outage_test")
    params = conninfo_to_dict(pg_url)
    params["dbname"] = "outage_test"
    url = make_conninfo(**params)
    migrate(url, verbose=False)
    return url
