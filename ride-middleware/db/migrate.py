"""Apply db/migrations/*.sql in order, once each. Works on Supabase and on the local postgres.
Owner: Person 2.

    python -m db.migrate                    # uses DATABASE_URL (see .env)
    python -m db.migrate --status           # show what is applied

Safe to run many times and from many machines at once (advisory xact lock + schema_migrations table).
"""
import argparse
import sys
from pathlib import Path

import psycopg

from common.config import DATABASE_URL

MIGRATIONS = Path(__file__).with_name("migrations")
LOCK_ID = 83170001


def migrate(url: str = DATABASE_URL, verbose: bool = True) -> list[str]:
    """Each file runs in its own transaction holding a TRANSACTION-level advisory lock, so it is safe
    with several runners at once AND through connection poolers (Supabase :6543), where a
    session-level lock could leak onto a pooled connection."""
    applied_now = []
    with psycopg.connect(url, autocommit=True, prepare_threshold=None) as conn:
        with conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_ID,))
            conn.execute("""CREATE TABLE IF NOT EXISTS schema_migrations (
                              version VARCHAR(100) PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())""")
        for f in sorted(MIGRATIONS.glob("*.sql")):
            with conn.transaction():
                conn.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_ID,))
                if conn.execute("SELECT 1 FROM schema_migrations WHERE version = %s", (f.name,)).fetchone():
                    continue                                    # already applied (maybe by another runner)
                conn.execute(f.read_text(encoding="utf-8"))
                conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (f.name,))
            applied_now.append(f.name)
            if verbose:
                print(f"applied {f.name}")
    if verbose:
        print("database is up to date" if not applied_now else f"{len(applied_now)} migration(s) applied")
    return applied_now


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--status", action="store_true")
    a = ap.parse_args()
    if a.status:
        with psycopg.connect(DATABASE_URL, prepare_threshold=None) as c:
            for v, t in c.execute("SELECT version, applied_at FROM schema_migrations ORDER BY 1"):
                print(f"{v:<30} {t:%Y-%m-%d %H:%M}")
        return 0
    migrate()
    return 0


if __name__ == "__main__":
    sys.exit(main())
