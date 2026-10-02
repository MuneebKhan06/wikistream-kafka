import json

from scripts.failure_tests import SCENARIOS, duplicate_ids
from scripts.faults import FAULTS


def record(event_id):
    return (0, 0, b"k", json.dumps({"event_id": event_id}).encode())


def test_duplicate_ids_reports_only_repeats():
    records = [record("a"), record("b"), record("a"), record("a")]
    assert duplicate_ids(records) == {"a": 3}
    assert duplicate_ids([record("a"), record("b")]) == {}


def test_every_scenario_says_what_it_proves():
    for name, function in SCENARIOS.items():
        assert function.__doc__ and function.__doc__.strip(), name


def test_fault_launchers_are_registered():
    assert set(FAULTS) == {"cleaner-mid-transaction", "sink-after-write"}
