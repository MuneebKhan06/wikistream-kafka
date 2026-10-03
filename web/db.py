"""Read only access to PostgreSQL for the dashboard.

A small pool, because every page refresh issues a few queries at once.
Each connection is read only and carries a statement timeout, so a slow
query fails fast instead of holding a worker, and nothing served here can
change the data the pipeline writes.
"""

import logging
import threading
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from common.config import POSTGRES

STATEMENT_TIMEOUT_MS = 5000
# Enough for an hour of live traffic to be grouped in memory, not on disk.
WORK_MEM = "32MB"
MAX_CONNECTIONS = 8

log = logging.getLogger("web.db")


class Database:
    def __init__(self, dsn: str = None, max_connections: int = MAX_CONNECTIONS):
        self.dsn = dsn or POSTGRES.dsn()
        self.max_connections = max_connections
        self._pool = None
        self._lock = threading.Lock()

    def _get_pool(self) -> ThreadedConnectionPool:
        # Created on first use, so the API starts even while PostgreSQL is down
        # and reports that through /api/health instead of failing to boot.
        with self._lock:
            if self._pool is None:
                self._pool = ThreadedConnectionPool(
                    1,
                    self.max_connections,
                    self.dsn,
                    options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS} -c work_mem={WORK_MEM}",
                )
            return self._pool

    @contextmanager
    def cursor(self):
        pool = self._get_pool()
        conn = pool.getconn()
        broken = False
        try:
            conn.set_session(readonly=True, autocommit=True)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                yield cur
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            broken = True
            raise
        finally:
            # A connection that failed at the transport level is discarded, so
            # the next request opens a fresh one after a database restart.
            pool.putconn(conn, close=broken or conn.closed != 0)

    def rows(self, sql: str, params=None) -> list:
        with self.cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

    def one(self, sql: str, params=None) -> dict:
        rows = self.rows(sql, params)
        return rows[0] if rows else {}

    def ping(self) -> bool:
        try:
            return self.one("SELECT 1 AS ok").get("ok") == 1
        except Exception as exc:
            log.warning("database ping failed: %s", exc)
            return False

    def close(self) -> None:
        with self._lock:
            if self._pool is not None:
                self._pool.closeall()
                self._pool = None
