"""Names are computed at import, so each case loads the config in a fresh process."""

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PROBE = """
import json
from common import config
from processors import cleaner, edit_war, page_state, trending
from sinks import alerts_sink, postgres_sink
from ingestor import checkpoint
print(json.dumps({
    "topics": [t.name for t in config.TOPICS],
    "groups": [cleaner.GROUP_ID, edit_war.GROUP_ID, page_state.GROUP_ID,
               trending.GROUP_ID, postgres_sink.GROUP_ID, alerts_sink.GROUP_ID],
    "txn": [cleaner.transactional_id("1"), edit_war.transactional_id("1"),
            page_state.transactional_id("1")],
    "assignor": config.consumer_config("g")["partition.assignment.strategy"],
    "cooperative": config.COOPERATIVE,
    "rf": config.REPLICATION_FACTOR,
    "isr": config.MIN_INSYNC_REPLICAS,
    "checkpoint": checkpoint.DEFAULT_PATH.name,
}))
"""


def names(**env):
    clean_env = {k: v for k, v in os.environ.items() if not k.startswith(("PIPELINE_", "KAFKA_"))}
    clean_env.update(env)
    out = subprocess.run(
        [sys.executable, "-c", PROBE],
        cwd=ROOT,
        env=clean_env,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


def test_default_namespace_keeps_production_names():
    result = names()
    assert result["topics"] == [
        "wiki.raw",
        "wiki.clean",
        "wiki.alerts",
        "wiki.page-latest",
        "wiki.dlq",
    ]
    assert result["groups"] == [
        "cleaner",
        "edit-wars",
        "page-state",
        "trending",
        "storage",
        "alerts-storage",
    ]
    assert result["txn"] == ["cleaner-1", "edit-war-1", "page-state-1"]
    assert (result["rf"], result["isr"]) == (3, "2")
    assert result["checkpoint"] == "ingestor.json"


def test_another_namespace_shares_nothing_with_production():
    result = names(PIPELINE_NAMESPACE="itest")
    assert all(t.startswith("itest.") for t in result["topics"])
    assert all(g.startswith("itest.") for g in result["groups"])
    # A shared transactional id would fence the real producer.
    assert all(t.startswith("itest.") for t in result["txn"])
    assert result["checkpoint"] == "itest-ingestor.json"


def test_replication_can_be_lowered_for_a_single_broker():
    result = names(KAFKA_REPLICATION_FACTOR="1", KAFKA_MIN_INSYNC_REPLICAS="1")
    assert (result["rf"], result["isr"]) == (1, "1")


def test_assignor_defaults_to_cooperative_sticky_and_can_be_changed():
    assert names()["assignor"] == "cooperative-sticky"
    assert names()["cooperative"] is True
    eager = names(KAFKA_ASSIGNOR="range")
    assert eager["assignor"] == "range"
    assert eager["cooperative"] is False
