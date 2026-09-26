"""Connection pools + retry helper, shared by every service that writes to PostgreSQL.
Owner: Person 1 (common/).

pool(url)                   one pooled set of connections per database (1..5 connections)
with_db_retry(fn, ...)      run fn(conn) in ONE transaction; if the DB is down/slow:
                              - max_retries=None : wait and retry forever  (booking data: never give up,
                                                   Kafka keeps the messages, consumer lag grows)
                              - max_retries=3    : retry 3 times then raise RetriesExhausted
                                                   (payment: guardrail -> caller moves message to DLQ)
Backoff: 1 s, 2 s, 4 s ... capped at 30 s. Every retry is logged as RETRYING and counted.
"""
import os
import time

import psycopg
from psycopg_pool import ConnectionPool, PoolTimeout

from common.config import DATABASE_URL, DB_POOL_MAX, DB_PREPARE_THRESHOLD
from common.health import beat
from common.metrics import DB_RETRIES, RETRIES

POOL_TIMEOUT = float(os.getenv("DB_POOL_TIMEOUT", "5"))
_pools: dict[str, ConnectionPool] = {}


class RetriesExhausted(Exception):
    """DB still failing after max_retries."""


def pool(url: str = DATABASE_URL) -> ConnectionPool:
    if url not in _pools:
        _pools[url] = ConnectionPool(
            url,
            min_size=1,
            max_size=DB_POOL_MAX,                     # connection pooling (brief requirement)
            kwargs={"prepare_threshold": DB_PREPARE_THRESHOLD},   # Supabase transaction-pooler safe
            timeout=POOL_TIMEOUT,
            check=ConnectionPool.check_connection,    # drop dead connections after a DB restart
            open=True,
        )
    return _pools[url]


def with_db_retry(fn, log, service: str, url: str = DATABASE_URL,
                  max_retries: int | None = None, max_wait: float = 30.0):
    attempt = 0
    while True:
        try:
            with pool(url).connection() as conn:      # commits on success, rolls back on error
                return fn(conn)
        except (psycopg.OperationalError, PoolTimeout) as e:
            attempt += 1
            reason = str(e).splitlines()[0][:200]
            if max_retries is not None and attempt > max_retries:
                raise RetriesExhausted(f"{attempt - 1} retries failed: {reason}") from e
            wait = min(2 ** (attempt - 1), max_wait)
            DB_RETRIES.labels(service).inc()
            RETRIES.labels(service, str(min(attempt, 9))).inc()
            log.retrying("database unavailable, retrying same batch",
                         attempt=attempt, max_retries=max_retries or "unlimited", wait_s=wait, error=reason)
            beat()                         # waiting for the DB is alive, not hung
            time.sleep(wait)
