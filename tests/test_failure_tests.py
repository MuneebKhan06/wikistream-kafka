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


def test_owned_partitions_follows_assign_and_revoke_lines():
    from scripts.failure_tests import owned_partitions

    log = "\n".join(
        [
            "INFO [cleaner] assigned partitions: [0, 1, 2, 3, 4, 5]",
            "INFO [cleaner] revoked partitions: [3, 4, 5]",
            "INFO [cleaner] revoked partitions: [2]",
            "INFO [cleaner] assigned partitions: []",
            "INFO [cleaner] stop requested, finishing current transaction",
            "INFO [cleaner] revoked partitions: [0, 1]",
        ]
    )
    # The shutdown revocation after "stop requested" is not a rebalance.
    assert owned_partitions(log) == [0, 1]
