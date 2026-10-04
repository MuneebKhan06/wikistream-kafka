"""Replays the recorded sample through the whole pipeline and checks every output.

Runs the real processes against a real cluster and PostgreSQL, isolated from
the running pipeline: its own namespace gives it separate topics, consumer
groups and transactional ids, and it gets its own database. Everything it
creates is removed afterwards.

The sample is replayed twice, so half the input is duplicates. On top of it
go cases the live stream cannot be relied on to produce: an edit war, two
edits to one page arriving newest first, a deletion arriving before its
page's edit, and one malformed record.

Sentinel events dated ten minutes later come last. They move the stream
past every earlier minute, so those trending windows close and are written
while the pipeline is running, which gives the test something real to wait
for. There are enough sentinel pages that every partition gets one.

Takes about a minute, so it only runs when asked for:
    pytest -m e2e
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from common.sandbox import Sandbox, group_caught_up, produce, services_available

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "samples" / "recentchange_sample.jsonl"
PROCESSES = [
    "processors/cleaner.py",
    "sinks/postgres_sink.py",
    "processors/edit_war.py",
    "sinks/alerts_sink.py",
    "processors/page_state.py",
    "processors/trending.py",
]
SENTINELS = 60
SENTINEL_SEC = 600

pytestmark = [pytest.mark.integration, pytest.mark.e2e]


# Synthetic input.


def base_time() -> datetime:
    """Synthetic events sit inside the sample's own minute, so none is late."""
    first = json.loads(SAMPLE.open(encoding="utf-8").readline())
    return datetime.fromisoformat(first["meta"]["dt"].replace("Z", "+00:00"))


def raw_event(event_id, title, seconds, user="Tester", comment="edit", **extra):
    dt = (base_time() + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
    event = {
        "meta": {"id": event_id, "dt": dt, "domain": "en.wikipedia.org"},
        "type": "edit",
        "namespace": 0,
        "title": title,
        "comment": comment,
        "user": user,
        "bot": False,
        "wiki": "enwiki",
        "server_name": "en.wikipedia.org",
    }
    event.update(extra)
    return event


def synthetic_cases(ns):
    war = f"Integration War {ns}"
    latest = f"Integration Latest {ns}"
    deleted = f"Integration Deleted {ns}"
    revert = "Reverted edits by X"
    war_events = [
        raw_event(f"{ns}-war-1", war, 10, "Alice", revert),
        raw_event(f"{ns}-war-2", war, 20, "Bob", revert),
        raw_event(f"{ns}-war-3", war, 30, "Alice", revert),
    ]
    # Produced newest first: the older edit must not win.
    latest_events = [
        raw_event(f"{ns}-latest-new", latest, 41, revision={"old": 100, "new": 101}),
        raw_event(f"{ns}-latest-old", latest, 40, revision={"old": 99, "new": 100}),
    ]
    # The deletion is produced before the edit it follows.
    deleted_events = [
        raw_event(
            f"{ns}-deleted-log", deleted, 45, type="log", log_type="delete", log_action="delete"
        ),
        raw_event(f"{ns}-deleted-edit", deleted, 5),
    ]
    sentinels = [
        raw_event(f"{ns}-sentinel-{i}", f"Integration Sentinel {ns} {i}", SENTINEL_SEC)
        for i in range(SENTINELS)
    ]
    return {
        "sentinels": sentinels,
        "sentinel_minute": (base_time() + timedelta(seconds=SENTINEL_SEC)).replace(
            second=0, microsecond=0
        ),
        "war_title": war,
        "war_first_id": war_events[0]["meta"]["id"],
        "latest_key": f"enwiki:{latest}",
        "deleted_key": f"enwiki:{deleted}",
        "events": war_events + latest_events + deleted_events,
    }


@pytest.fixture
def run(tmp_path):
    if not services_available():
        pytest.skip("Kafka or PostgreSQL is not running")
    with Sandbox("itest", logs=tmp_path) as sandbox:
        yield sandbox


def test_sample_flows_through_the_whole_pipeline(run):
    from common.models import clean_event, parse_raw
    from processors.edit_war_detector import alert_id_for
    from processors.page_latest import page_update

    ns = run.ns
    cases = synthetic_cases(ns)

    run.python("admin/create_topics.py")
    for script in PROCESSES:
        run.start(script)

    # The sample twice, then the synthetic cases in their deliberate order.
    run.python("scripts/replay.py", "--topic", f"{ns}.raw", "--loops", "2")
    produce(
        f"{ns}.raw",
        [(e["meta"]["id"].encode(), json.dumps(e).encode()) for e in cases["events"]]
        + [(b"malformed", b'{"meta": ')],
    )

    sample = [clean_event(parse_raw(line)) for line in SAMPLE.open("rb") if line.strip()]
    synthetic = [clean_event(e) for e in cases["events"]]
    sentinels = [clean_event(e) for e in cases["sentinels"]]
    before_sentinels = len(sample) + len(synthetic)
    expected_events = before_sentinels + len(sentinels)

    stored = "SELECT count(*) FROM edits"
    run.wait_until(
        "the sample and synthetic events stored",
        lambda: run.query(stored)[0][0] == before_sentinels,
    )
    # Only now the sentinels: later events arrive later. Sent sooner, one
    # could overtake an earlier event inside the cleaner and make it late.
    produce(
        f"{ns}.raw",
        [(e["meta"]["id"].encode(), json.dumps(e).encode()) for e in cases["sentinels"]],
    )
    run.wait_until("every event stored", lambda: run.query(stored)[0][0] == expected_events)
    run.wait_until("the edit war alert", lambda: run.query("SELECT count(*) FROM alerts")[0][0] >= 1)
    run.wait_until(
        "page state to catch up",
        lambda: group_caught_up(f"{ns}.page-state", f"{ns}.clean"),
    )
    # Windows before the sentinels closed and were written in normal operation.
    closed_total = (
        "SELECT COALESCE(SUM(edit_count), 0) FROM trending_minutes WHERE minute < %s"
    )
    run.wait_until(
        "trending to close and write the earlier minutes",
        lambda: run.query(closed_total, (cases["sentinel_minute"],))[0][0] == before_sentinels,
    )
    run.stop_all()

    # Every event stored exactly once, although half the input was duplicates.
    rows, unique = run.query("SELECT count(*), count(DISTINCT event_id) FROM edits")[0]
    assert rows == unique == expected_events

    # The malformed record went to the DLQ, and only it.
    from scripts.check_duplicates import scan

    dlq = scan(f"{ns}.dlq", "stage")
    assert dlq["records"] == 1
    # The dashboard counts it the same way, not by offsets: the cleaner writes
    # the DLQ in transactions, and each commit marker takes an offset too.
    from web.cluster import count_records

    assert count_records(f"{ns}.dlq", 1) == 1

    clean = scan(f"{ns}.clean")
    assert clean["records"] == expected_events
    assert clean["duplicates"] == {}

    # Exactly the synthetic war was flagged, under its deterministic id.
    alerts = run.query("SELECT alert_id, title, revert_count FROM alerts")
    war = [a for a in alerts if a[1] == cases["war_title"]]
    assert len(war) == 1
    assert war[0][0] == alert_id_for("enwiki", cases["war_title"], cases["war_first_id"])
    assert war[0][2] == 3

    # The compacted topic holds the newest edit per page, and no deleted page.
    from scripts.page_snapshot import build_snapshot

    pages = build_snapshot(f"{ns}.page-latest")["pages"]
    assert pages[cases["latest_key"]].rev_id == 101
    assert cases["deleted_key"] not in pages
    expected_pages = set()
    for event in sorted(sample + synthetic + sentinels, key=lambda e: e.event_time):
        update = page_update(event)
        if update is None:
            continue
        key, record = update
        (expected_pages.add if record is not None else expected_pages.discard)(key)
    assert set(pages) == expected_pages

    # The sentinels' own minute was still open, and shutdown wrote it too.
    counted = run.query("SELECT COALESCE(SUM(edit_count), 0) FROM trending_minutes")[0][0]
    assert counted == expected_events
