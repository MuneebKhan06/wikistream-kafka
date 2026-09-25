"""PostgreSQL helpers for the sinks.

Every insert is an upsert keyed by event_id. Kafka transactions stop at the
Kafka boundary, so the sink commits offsets only after the rows are
committed here. A crash between the two can therefore only replay a batch,
never lose it, and replaying is harmless because the write is idempotent.
"""

import logging
from contextlib import contextmanager
from typing import Iterable, Sequence

import psycopg2
from psycopg2.extras import execute_values

from common.config import POSTGRES
from common.models import CleanEvent

INSERT_EDITS = """
    INSERT INTO edits (
        event_id, wiki, title, type, namespace, username, bot, minor,
        comment, event_time, rev_old, rev_new, length_old, length_new,
        server_name
    )
    VALUES %s
    ON CONFLICT (event_id) DO NOTHING
"""

UPSERT_TRENDING = """
    INSERT INTO trending_minutes (wiki, title, minute, edit_count)
    VALUES %s
    ON CONFLICT (wiki, title, minute)
    DO UPDATE SET edit_count = trending_minutes.edit_count + EXCLUDED.edit_count
"""


def connect(dsn: str = None):
    connection = psycopg2.connect(dsn or POSTGRES.dsn())
    connection.autocommit = False
    return connection


@contextmanager
def cursor(connection):
    """One transaction per block: commit on success, roll back on failure."""
    with connection:
        with connection.cursor() as cur:
            yield cur


def as_row(event: CleanEvent) -> tuple:
    return (
        event.event_id,
        event.wiki,
        event.title,
        event.type,
        event.namespace,
        event.user,
        event.bot,
        event.minor,
        event.comment,
        event.event_time,
        event.rev_old,
        event.rev_new,
        event.length_old,
        event.length_new,
        event.server_name,
    )


def insert_edits(connection, events: Iterable[CleanEvent]) -> int:
    """Insert a batch of events, skipping ones already stored."""
    rows = [as_row(event) for event in events]
    if not rows:
        return 0
    with cursor(connection) as cur:
        execute_values(cur, INSERT_EDITS, rows, page_size=500)
        return cur.rowcount


def upsert_trending(connection, counts: Sequence[tuple]) -> int:
    """Add per minute counts, summing into any row already there."""
    if not counts:
        return 0
    with cursor(connection) as cur:
        execute_values(cur, UPSERT_TRENDING, list(counts), page_size=500)
        return cur.rowcount


def wait_for_database(retries: int = 10, delay: float = 2.0, log: logging.Logger = None):
    """Postgres may still be starting when a sink launches."""
    import time

    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return connect()
        except psycopg2.OperationalError as exc:
            last_error = exc
            if log:
                log.warning("database not ready (attempt %d/%d)", attempt, retries)
            time.sleep(delay)
    raise last_error
