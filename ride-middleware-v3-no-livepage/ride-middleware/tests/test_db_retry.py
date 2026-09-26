"""DB outage handling (common/db.py) against a REAL PostgreSQL.

- booking data: retry forever until the DB is back (nothing lost, consumer lag grows)
- payment data: retry MAX_RETRIES times, then RetriesExhausted -> caller moves message to DLQ
The outage is real: the database is switched to "no connections allowed" and all sessions killed.
"""
import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from common import db as dbmod


class RecordingLog:
    def __init__(self):
        self.retries = []

    def retrying(self, msg, **f):
        self.retries.append(f["attempt"])


@pytest.fixture()
def fast(monkeypatch):
    monkeypatch.setattr(dbmod.time, "sleep", lambda s: None)       # don't really wait in tests
    monkeypatch.setattr(dbmod, "POOL_TIMEOUT", 1.0)
    monkeypatch.setattr(dbmod, "_pools", {})
    yield
    for p in dbmod._pools.values():
        p.close()


def set_db_online(admin_url: str, dbname: str, online: bool):
    """Simulate the DB going down / coming back (needs a superuser, e.g. docker 'app' or pgserver)."""
    params = conninfo_to_dict(admin_url)
    params["dbname"] = "postgres"
    with psycopg.connect(make_conninfo(**params), autocommit=True) as c:
        c.execute(f"ALTER DATABASE {dbname} WITH ALLOW_CONNECTIONS {'true' if online else 'false'}")
        if not online:
            c.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s", (dbname,))


def test_retries_forever_until_database_is_back(pg_url, monkeypatch, fast):
    """Booking path: first 3 attempts fail, then the DB is back -> the batch succeeds."""
    good = psycopg.connect(pg_url, autocommit=True)
    calls = {"n": 0}

    class FlakyPool:
        def connection(self):
            calls["n"] += 1
            if calls["n"] <= 3:
                raise psycopg.OperationalError("connection refused (simulated outage)")
            return good

    monkeypatch.setattr(dbmod, "pool", lambda url=None: FlakyPool())
    log = RecordingLog()
    assert dbmod.with_db_retry(lambda c: c.execute("select 42").fetchone()[0], log, "test") == 42
    assert log.retries == [1, 2, 3]


def test_real_outage_retry_limit_then_recovery(scratch_db_url, fast):
    """Payment path: real outage -> RETRYING x3 -> RetriesExhausted (-> DLQ). DB back -> works again."""
    pay_url = scratch_db_url
    log = RecordingLog()
    assert dbmod.with_db_retry(lambda c: c.execute("select 1").fetchone()[0], log, "t", url=pay_url) == 1

    set_db_online(pay_url, "outage_test", online=False)
    try:
        with pytest.raises(dbmod.RetriesExhausted):
            dbmod.with_db_retry(lambda c: c.execute("select 1").fetchone()[0], log, "t",
                                url=pay_url, max_retries=3)
        assert log.retries == [1, 2, 3]                                # exactly 3 retries, then give up
    finally:
        set_db_online(pay_url, "outage_test", online=True)

    assert dbmod.with_db_retry(lambda c: c.execute("select 2").fetchone()[0], log, "t",
                               url=pay_url, max_retries=3) == 2        # pool reconnects by itself
