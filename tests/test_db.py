import json

import pytest

from common.models import clean_event, parse_raw
from sinks.db import as_row, insert_edits, upsert_trending

RAW = {
    "meta": {"id": "e-1", "dt": "2026-09-25T10:00:00Z"},
    "type": "edit",
    "namespace": 0,
    "title": "Roma",
    "comment": "fix typo",
    "timestamp": 1790000000,
    "user": "Alice",
    "bot": False,
    "minor": True,
    "wiki": "itwiki",
    "server_name": "it.wikipedia.org",
    "length": {"old": 10, "new": 25},
    "revision": {"old": 1, "new": 2},
}


def sample_event(**overrides):
    raw = dict(RAW, **overrides)
    return clean_event(parse_raw(json.dumps(raw).encode()))


def test_row_matches_insert_column_order():
    row = as_row(sample_event())
    assert row[0] == "e-1"
    assert row[1:4] == ("itwiki", "Roma", "edit")
    assert row[5] == "Alice"
    assert row[6:8] == (False, True)
    assert row[10:14] == (1, 2, 10, 25)
    assert len(row) == 15


def test_empty_batches_touch_no_connection():
    assert insert_edits(None, []) == 0
    assert upsert_trending(None, []) == 0


@pytest.fixture
def connection():
    psycopg2 = pytest.importorskip("psycopg2")
    from sinks.db import connect

    try:
        conn = connect()
    except psycopg2.OperationalError:
        pytest.skip("postgres not running")
    yield conn
    conn.close()


@pytest.mark.integration
def test_insert_is_idempotent(connection):
    events = [sample_event(meta={"id": "test-idem-1", "dt": "2026-09-25T10:00:00Z"})]
    try:
        assert insert_edits(connection, events) == 1
        assert insert_edits(connection, events) == 0
    finally:
        with connection.cursor() as cur:
            cur.execute("DELETE FROM edits WHERE event_id = 'test-idem-1'")
        connection.commit()


@pytest.mark.integration
def test_trending_write_keeps_the_larger_total(connection):
    """A replay recomputes a window and must not lower a complete total."""
    row = ("testwiki", "Replay Page", "2026-09-26T10:00:00+00:00")
    try:
        assert upsert_trending(connection, [row + (10,)]) == 1
        # A partial recount after a restart sees fewer events.
        upsert_trending(connection, [row + (4,)])
        with connection.cursor() as cur:
            cur.execute(
                "SELECT edit_count FROM trending_minutes "
                "WHERE wiki = %s AND title = %s",
                row[:2],
            )
            assert cur.fetchone()[0] == 10

        # A later, more complete count does raise it.
        upsert_trending(connection, [row + (17,)])
        with connection.cursor() as cur:
            cur.execute(
                "SELECT edit_count FROM trending_minutes "
                "WHERE wiki = %s AND title = %s",
                row[:2],
            )
            assert cur.fetchone()[0] == 17
    finally:
        with connection.cursor() as cur:
            cur.execute("DELETE FROM trending_minutes WHERE wiki = 'testwiki'")
        connection.commit()
