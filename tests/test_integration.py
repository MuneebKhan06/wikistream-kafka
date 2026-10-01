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
import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pytest

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
TIMEOUT_SEC = 180
SENTINELS = 60
SENTINEL_SEC = 600

pytestmark = [pytest.mark.integration, pytest.mark.e2e]


def services_available() -> bool:
    import psycopg2
    from confluent_kafka.admin import AdminClient

    from common.config import POSTGRES, admin_config

    try:
        AdminClient(admin_config()).list_topics(timeout=5)
        psycopg2.connect(POSTGRES.dsn(), connect_timeout=5).close()
    except Exception:
        return False
    return True


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


# Environment for one isolated run.


class Run:
    def __init__(self, tmp_path):
        self.ns = f"itest{uuid.uuid4().hex[:8]}"
        self.db = f"wikistream_{self.ns}"
        self.env = dict(os.environ, PIPELINE_NAMESPACE=self.ns, POSTGRES_DB=self.db)
        self.logs = tmp_path
        self.procs = {}

    def python(self, script, *args, check=True):
        return subprocess.run(
            [sys.executable, str(ROOT / script), *args],
            cwd=ROOT,
            env=self.env,
            capture_output=True,
            text=True,
            check=check,
            timeout=TIMEOUT_SEC,
        )

    def start(self, script):
        log = open(self.logs / f"{Path(script).stem}.log", "w")
        self.procs[script] = (
            subprocess.Popen(
                [sys.executable, str(ROOT / script)],
                cwd=ROOT,
                env=self.env,
                stdout=log,
                stderr=subprocess.STDOUT,
            ),
            log,
        )

    def stop_all(self):
        for proc, _ in self.procs.values():
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
        for proc, log in self.procs.values():
            try:
                proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                proc.kill()
            log.close()

    def crashed(self):
        return [s for s, (p, _) in self.procs.items() if p.poll() not in (None, 0)]

    def tails(self):
        out = []
        for path in sorted(self.logs.glob("*.log")):
            lines = path.read_text(errors="replace").splitlines()[-8:]
            out.append(f"--- {path.name}\n" + "\n".join(lines))
        return "\n".join(out)


def admin_connection(dbname="postgres"):
    import psycopg2

    from common.config import POSTGRES

    conn = psycopg2.connect(
        host=POSTGRES.host,
        port=POSTGRES.port,
        user=POSTGRES.user,
        password=POSTGRES.password,
        dbname=dbname,
    )
    conn.autocommit = True
    return conn


def create_database(name):
    conn = admin_connection()
    with conn.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{name}"')
    conn.close()
    conn = admin_connection(name)
    with conn.cursor() as cur:
        cur.execute((ROOT / "db" / "schema.sql").read_text())
    conn.close()


def drop_database(name):
    conn = admin_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s", (name,)
        )
        cur.execute(f'DROP DATABASE IF EXISTS "{name}"')
    conn.close()


def query(run, sql, params=None):
    conn = admin_connection(run.db)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    conn.close()
    return rows


def delete_kafka_state(ns):
    from confluent_kafka.admin import AdminClient

    from common.config import admin_config

    admin = AdminClient(admin_config())
    topics = [t for t in admin.list_topics(timeout=10).topics if t.startswith(f"{ns}.")]
    if topics:
        for future in admin.delete_topics(topics).values():
            try:
                future.result()
            except Exception:
                pass
    groups = [
        g.group_id
        for g in admin.list_consumer_groups().result().valid
        if g.group_id.startswith(f"{ns}.")
    ]
    if groups:
        for future in admin.delete_consumer_groups(groups).values():
            try:
                future.result()
            except Exception:
                pass


def produce(topic, records):
    from confluent_kafka import Producer

    from common.config import producer_config

    producer = Producer(producer_config())
    for key, value in records:
        producer.produce(topic, key=key, value=value)
        producer.flush(30)
    assert producer.flush(30) == 0


def group_caught_up(group, topic):
    """Committed offsets of a group have reached the end of every partition."""
    from confluent_kafka import Consumer, TopicPartition

    from common.config import consumer_config

    consumer = Consumer(consumer_config(group, **{"group.id": group}))
    try:
        count = len(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
        parts = [TopicPartition(topic, p) for p in range(count)]
        for tp in consumer.committed(parts, timeout=10):
            _, high = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
            if tp.offset < 0 and high > 0:
                return False
            if 0 <= tp.offset < high:
                return False
        return True
    finally:
        consumer.close()


def wait_for(run, description, condition):
    deadline = time.time() + TIMEOUT_SEC
    while time.time() < deadline:
        crashed = run.crashed()
        assert not crashed, f"{crashed} exited early\n{run.tails()}"
        try:
            if condition():
                return
        except Exception:
            pass
        time.sleep(2)
    pytest.fail(f"timed out waiting for {description}\n{run.tails()}")


@pytest.fixture
def run(tmp_path):
    if not services_available():
        pytest.skip("Kafka or PostgreSQL is not running")
    current = Run(tmp_path)
    create_database(current.db)
    try:
        yield current
    finally:
        current.stop_all()
        delete_kafka_state(current.ns)
        drop_database(current.db)


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
    wait_for(
        run,
        "the sample and synthetic events stored",
        lambda: query(run, stored)[0][0] == before_sentinels,
    )
    # Only now the sentinels: later events arrive later. Sent sooner, one
    # could overtake an earlier event inside the cleaner and make it late.
    produce(
        f"{ns}.raw",
        [(e["meta"]["id"].encode(), json.dumps(e).encode()) for e in cases["sentinels"]],
    )
    wait_for(run, "every event stored", lambda: query(run, stored)[0][0] == expected_events)
    wait_for(run, "the edit war alert", lambda: query(run, "SELECT count(*) FROM alerts")[0][0] >= 1)
    wait_for(
        run,
        "page state to catch up",
        lambda: group_caught_up(f"{ns}.page-state", f"{ns}.clean"),
    )
    # Windows before the sentinels closed and were written in normal operation.
    closed_total = (
        "SELECT COALESCE(SUM(edit_count), 0) FROM trending_minutes WHERE minute < %s"
    )
    wait_for(
        run,
        "trending to close and write the earlier minutes",
        lambda: query(run, closed_total, (cases["sentinel_minute"],))[0][0] == before_sentinels,
    )
    run.stop_all()

    # Every event stored exactly once, although half the input was duplicates.
    rows, unique = query(run, "SELECT count(*), count(DISTINCT event_id) FROM edits")[0]
    assert rows == unique == expected_events

    # The malformed record went to the DLQ, and only it.
    from scripts.check_duplicates import scan

    dlq = scan(f"{ns}.dlq", "stage")
    assert dlq["records"] == 1

    clean = scan(f"{ns}.clean")
    assert clean["records"] == expected_events
    assert clean["duplicates"] == {}

    # Exactly the synthetic war was flagged, under its deterministic id.
    alerts = query(run, "SELECT alert_id, title, revert_count FROM alerts")
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
    counted = query(run, "SELECT COALESCE(SUM(edit_count), 0) FROM trending_minutes")[0][0]
    assert counted == expected_events
