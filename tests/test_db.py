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
    assert row[15] is False
    assert len(row) == 16


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


def make_alert(alert_id="test-alert-1"):
    from processors.edit_war_detector import EditWarAlert

    return EditWarAlert(
        alert_id=alert_id,
        wiki="testwiki",
        title="Contested Page",
        revert_count=3,
        users=("Alice", "Bob"),
        event_ids=("a", "b", "c"),
        window_start="2026-09-27T10:00:00+00:00",
        window_end="2026-09-27T10:05:00+00:00",
    )


def test_empty_alert_batch_touches_no_connection():
    from sinks.db import insert_alerts

    assert insert_alerts(None, []) == 0


@pytest.mark.integration
def test_alert_insert_is_idempotent(connection):
    from sinks.db import insert_alerts

    alert = make_alert()
    try:
        assert insert_alerts(connection, [alert]) == 1
        assert insert_alerts(connection, [alert]) == 0
        with connection.cursor() as cur:
            cur.execute("SELECT users FROM alerts WHERE alert_id = %s", (alert.alert_id,))
            assert cur.fetchone()[0] == ["Alice", "Bob"]
    finally:
        with connection.cursor() as cur:
            cur.execute("DELETE FROM alerts WHERE wiki = 'testwiki'")
        connection.commit()


# Reconnecting, with a fake connection so no server has to be restarted.


class FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def make_database(monkeypatch, connections):
    import logging

    import psycopg2

    from sinks import db

    opened = []

    def fake_wait(retries=10, delay=2.0, log=None):
        if not connections:
            raise psycopg2.OperationalError("server still down")
        conn = connections.pop(0)
        opened.append(conn)
        return conn

    monkeypatch.setattr(db, "wait_for_database", fake_wait)
    return db.Database(logging.getLogger("t")), opened


def test_a_write_on_a_healthy_connection_just_runs(monkeypatch):
    database, opened = make_database(monkeypatch, [FakeConnection()])
    assert database.write(lambda conn, rows: len(rows), [1, 2, 3]) == 3
    assert database.reconnects == 0
    assert len(opened) == 1


def test_a_lost_connection_is_reopened_and_the_same_write_retried(monkeypatch):
    import psycopg2

    first, second = FakeConnection(), FakeConnection()
    database, _ = make_database(monkeypatch, [first, second])
    calls = []

    def write(conn, rows):
        calls.append((conn, rows))
        if conn is first:
            raise psycopg2.OperationalError("server closed the connection unexpectedly")
        return len(rows)

    assert database.write(write, ["a", "b"]) == 2
    assert first.closed is True
    assert database.reconnects == 1
    # The retry carried exactly the same batch.
    assert [rows for _, rows in calls] == [["a", "b"], ["a", "b"]]


def test_giving_up_raises_so_offsets_stay_uncommitted(monkeypatch):
    import psycopg2

    database, _ = make_database(monkeypatch, [FakeConnection()])

    def write(conn, rows):
        raise psycopg2.InterfaceError("connection already closed")

    with pytest.raises(psycopg2.OperationalError):
        database.write(write, ["a"])


def test_errors_that_are_not_about_the_connection_are_not_retried(monkeypatch):
    database, _ = make_database(monkeypatch, [FakeConnection(), FakeConnection()])

    def write(conn, rows):
        raise ValueError("bad row")

    with pytest.raises(ValueError):
        database.write(write, ["a"])
    assert database.reconnects == 0
