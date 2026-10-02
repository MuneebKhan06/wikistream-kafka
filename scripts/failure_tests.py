"""Runs the failure tests against the real cluster and records what happened.

Each scenario builds an isolated sandbox, breaks one thing at a precise
moment, lets the pipeline recover, and then checks the data: counts,
duplicates and gaps, read back from Kafka and PostgreSQL. A scenario passes
on evidence, not on the absence of an exception.

Usage:
  python scripts/failure_tests.py                    # every scenario
  python scripts/failure_tests.py cleaner-crash sink-crash
  python scripts/failure_tests.py --list

Results are printed and merged into benchmarks/failure_tests.json.
"""

import argparse
import json
import re
import subprocess
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.config import PROJECT_ROOT, STATE_DIR  # noqa: E402
from common.metrics import setup_logging  # noqa: E402
from common.sandbox import Sandbox, committed_offsets, group_caught_up  # noqa: E402
from scripts.faults import CRASH_EXIT_CODE  # noqa: E402

log = setup_logging("failure-tests")

RESULTS = PROJECT_ROOT / "benchmarks" / "failure_tests.json"
SAMPLE_EVENTS = 2000


def read_topic(topic: str, isolation: str = "read_committed") -> list:
    """Every record of a topic, as (partition, offset, key, value)."""
    from confluent_kafka import Consumer, KafkaError, KafkaException

    from common.config import consumer_config

    group = f"failure-tests-{uuid.uuid4().hex[:8]}"
    consumer = Consumer(
        consumer_config(
            group,
            **{"group.id": group, "enable.partition.eof": True, "isolation.level": isolation},
        )
    )
    consumer.subscribe([topic])
    partitions = len(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
    records, finished = [], set()
    try:
        while len(finished) < partitions:
            msg = consumer.poll(15)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    finished.add(msg.partition())
                    continue
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                continue
            records.append((msg.partition(), msg.offset(), msg.key(), msg.value()))
    finally:
        consumer.close()
    return records


def duplicate_ids(records: list, field: str = "event_id") -> dict:
    counts = Counter(json.loads(value)[field] for _, _, _, value in records)
    return {key: count for key, count in counts.items() if count > 1}


def cleaner_crash() -> dict:
    """Kill the cleaner mid transaction; wiki.clean must hold no duplicates."""
    loops = 3
    with Sandbox("ft", database=False) as box:
        box.python("admin/create_topics.py")
        box.python("scripts/replay.py", "--topic", box.topic("raw"), "--loops", str(loops))

        crasher = box.start("scripts/faults.py", "cleaner-mid-transaction", "2", name="crasher")
        exit_code = box.wait_exit(crasher)
        visible = len(read_topic(box.topic("clean")))
        in_log = len(read_topic(box.topic("clean"), isolation="read_uncommitted"))

        cleaner = box.start("processors/cleaner.py")
        box.wait_until(
            "the restarted cleaner to catch up",
            lambda: group_caught_up(box.group("cleaner"), box.topic("raw")),
            ignore_exits=(crasher,),
        )
        box.stop(cleaner)

        records = read_topic(box.topic("clean"))
        duplicates = duplicate_ids(records)
        unique = len({json.loads(v)["event_id"] for _, _, _, v in records})
        return {
            "action": "cleaner exits mid transaction: output produced, offsets sent, no commit",
            "raw_records": SAMPLE_EVENTS * loops,
            "unique_events": SAMPLE_EVENTS,
            "crash_exit_code": exit_code,
            "after_crash_visible_to_read_committed": visible,
            "after_crash_written_to_the_log": in_log,
            "uncommitted_records_left_by_the_crash": in_log - visible,
            "clean_records_after_restart": len(records),
            "clean_unique_event_ids": unique,
            "duplicate_event_ids": len(duplicates),
            "passed": exit_code == CRASH_EXIT_CODE
            and in_log > visible
            and len(records) == unique == SAMPLE_EVENTS
            and not duplicates,
        }


def sink_crash() -> dict:
    """Kill the sink after the DB write, before the offset commit; no duplicate rows."""
    with Sandbox("ft") as box:
        box.python("admin/create_topics.py")
        box.python("scripts/replay.py", "--topic", box.topic("raw"))
        cleaner = box.start("processors/cleaner.py")
        box.wait_until(
            "the cleaner to fill the clean topic",
            lambda: group_caught_up(box.group("cleaner"), box.topic("raw")),
        )
        box.stop(cleaner)

        crasher = box.start("scripts/faults.py", "sink-after-write", "1", name="crasher")
        exit_code = box.wait_exit(crasher)
        rows_after_crash = box.query("SELECT count(*) FROM edits")[0][0]
        before = committed_offsets(box.group("storage"), box.topic("clean"))
        partitions_committed = sum(1 for committed, _ in before.values() if committed >= 0)

        sink = box.start("sinks/postgres_sink.py")
        box.wait_until(
            "the restarted sink to store every event",
            lambda: box.query("SELECT count(*) FROM edits")[0][0] == SAMPLE_EVENTS,
            ignore_exits=(crasher,),
        )
        box.stop(sink)
        # A transactional topic ends each partition with a commit marker, so
        # a group that has read everything sits one offset short of the end.
        after = committed_offsets(box.group("storage"), box.topic("clean"))
        uncommitted = sum(max(end - committed - 1, 0) for committed, end in after.values())

        rows, unique = box.query("SELECT count(*), count(DISTINCT event_id) FROM edits")[0]
        summary = [line for line in box.log_text(sink).splitlines() if "already present" in line]
        already_present = int(summary[-1].split("inserted, ")[1].split(" ")[0]) if summary else -1
        return {
            "action": "sink exits after PostgreSQL commits a batch, before committing offsets",
            "events": SAMPLE_EVENTS,
            "crash_exit_code": exit_code,
            "rows_written_before_the_crash": rows_after_crash,
            "partitions_with_offsets_committed_before_the_crash": partitions_committed,
            "records_left_uncommitted_after_restart": uncommitted,
            "rows_reprocessed_and_skipped_after_restart": already_present,
            "rows_after_restart": rows,
            "unique_event_ids": unique,
            "passed": exit_code == CRASH_EXIT_CODE
            and rows_after_crash > 0
            and partitions_committed == 0
            and uncommitted == 0
            and already_present == rows_after_crash
            and rows == unique == SAMPLE_EVENTS,
        }


def ingestor_crash() -> dict:
    """SIGKILL the ingestor on the live stream; no gap in the source's own offsets."""
    run_sec = 25
    with Sandbox("ft", database=False) as box:
        checkpoint = STATE_DIR / f"{box.ns}-ingestor.json"
        try:
            box.python("admin/create_topics.py")
            first = box.start("ingestor/main.py", name="ingestor-1")
            time.sleep(run_sec)
            box.kill(first)
            confirmed_at_crash = json.loads(checkpoint.read_text())["events_confirmed"]
            in_raw_at_crash = len(read_topic(box.topic("raw")))

            second = box.start("ingestor/main.py", name="ingestor-2")
            time.sleep(run_sec)
            box.stop(second)
            resumed = "resuming after" in box.log_text(second)

            records = read_topic(box.topic("raw"))
        finally:
            checkpoint.unlink(missing_ok=True)

        # The source stamps every event with its own topic, partition and
        # offset. A gap in those offsets is an event the ingestor skipped.
        seen = {}
        ids = Counter()
        for _, _, _, value in records:
            meta = json.loads(value)["meta"]
            ids[meta["id"]] += 1
            seen.setdefault((meta["topic"], meta["partition"]), set()).add(meta["offset"])
        gaps = 0
        for offsets in seen.values():
            gaps += (max(offsets) - min(offsets) + 1) - len(offsets)
        duplicates = sum(count - 1 for count in ids.values() if count > 1)
        return {
            "action": "ingestor killed with SIGKILL on the live stream, then restarted",
            "seconds_before_and_after": run_sec,
            "confirmed_in_checkpoint_at_crash": confirmed_at_crash,
            "records_in_raw_at_crash": in_raw_at_crash,
            "resumed_from_checkpoint": resumed,
            "records_in_raw": len(records),
            "unique_events": len(ids),
            "events_missing_from_the_source_sequence": gaps,
            "duplicates_from_the_restart": duplicates,
            "duplicate_rate": round(duplicates / len(records), 4) if records else 0,
            "passed": resumed and gaps == 0 and len(ids) > confirmed_at_crash,
        }


def broker_script(env: dict, *args) -> str:
    """Run scripts/kill_broker.sh and return its output."""
    done = subprocess.run(
        [str(PROJECT_ROOT / "scripts" / "kill_broker.sh"), *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    return done.stdout + done.stderr


def milliseconds(output: str, marker: str) -> int:
    match = re.search(rf"{marker} (\d+) ms", output)
    return int(match.group(1)) if match else -1


def restore_cluster(env: dict) -> None:
    """Bring every broker back and spread leadership again."""
    for broker in (1, 2, 3):
        running = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", f"kafka-{broker}"],
            capture_output=True,
            text=True,
        ).stdout.strip()
        if running != "true":
            subprocess.run(["docker", "start", f"kafka-{broker}"], capture_output=True)
    subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "admin" / "wait_for_leaders.py"), "insync",
         str(int(time.time() * 1000)), "120"],
        env=env,
        capture_output=True,
    )
    broker_script(env, "rebalance")


def broker_failure() -> dict:
    """Hard kill one of three brokers while producing; no acknowledged write lost."""
    rate, loops = 2000, 15
    with Sandbox("ft", database=False) as box:
        try:
            box.python("admin/create_topics.py")
            producer = box.start(
                "scripts/replay.py",
                "--topic", box.topic("raw"), "--rate", str(rate), "--loops", str(loops), "--json",
                name="producer",
            )
            time.sleep(5)
            killed = broker_script(box.env, "kill", "2")
            exit_code = box.wait_exit(producer)
            log_text = box.log_text(producer)
            outcome = json.loads(log_text[log_text.index("{\n"):])

            records = read_topic(box.topic("raw"))
            restarted = broker_script(box.env, "start", "2")
        finally:
            restore_cluster(box.env)

        return {
            "action": "broker 2 killed with SIGKILL while producing with acks=all",
            "produce_rate_per_sec": rate,
            "sent": outcome["sent"],
            "acknowledged": outcome["delivered"],
            "failed": outcome["failed"],
            "records_in_topic": len(records),
            "leadership_moved_ms": milliseconds(killed, "leadership moved off broker 2 in"),
            "replicas_back_in_sync_ms": milliseconds(restarted, "back in sync after"),
            "passed": exit_code == 0
            and outcome["failed"] == 0
            and outcome["delivered"] == outcome["sent"] == len(records),
        }


def two_brokers_down() -> dict:
    """Two of three brokers down; writes are refused, and none acknowledged is lost."""
    from confluent_kafka import Producer

    from common.config import producer_config

    attempts = 200
    with Sandbox("ft", database=False) as box:
        topic = box.topic("raw")
        outcomes = {}

        def send(prefix: str, count: int, timeout_ms: int) -> None:
            producer = Producer(producer_config(**{"message.timeout.ms": timeout_ms}))

            def on_delivery(err, msg):
                outcomes[msg.key().decode()] = "acknowledged" if err is None else err.name()

            for i in range(count):
                key = f"{prefix}-{i}"
                producer.produce(topic, key=key.encode(), value=key.encode(), on_delivery=on_delivery)
            producer.flush(timeout_ms / 1000 + 30)

        try:
            box.python("admin/create_topics.py")
            send("before", 100, 30000)

            broker_script(box.env, "kill", "2")
            subprocess.run(["docker", "kill", "kafka-3"], capture_output=True)
            time.sleep(3)
            send("during", attempts, 20000)
        finally:
            restore_cluster(box.env)

        present = {key.decode() for _, _, key, _ in read_topic(topic)}
        during = {k: v for k, v in outcomes.items() if k.startswith("during")}
        acknowledged = {k for k, v in during.items() if v == "acknowledged"}
        refused = {k for k in during if k not in acknowledged}
        before_ok = sum(1 for k, v in outcomes.items() if k.startswith("before") and v == "acknowledged")
        return {
            "action": "brokers 2 and 3 killed, then 200 writes attempted with acks=all",
            "acknowledged_before_the_outage": before_ok,
            "attempted_during_the_outage": attempts,
            "acknowledged_during_the_outage": len(acknowledged),
            "refused_during_the_outage": len(refused),
            "refusal_errors": dict(Counter(during[k] for k in refused)),
            "acknowledged_writes_missing_after_recovery": len(
                {k for k, v in outcomes.items() if v == "acknowledged"} - present
            ),
            "refused_writes_that_appeared_after_recovery": len(refused & present),
            "passed": before_ok == 100
            and len(refused) > 0
            and not ({k for k, v in outcomes.items() if v == "acknowledged"} - present),
        }


SCENARIOS = {
    "cleaner-crash": cleaner_crash,
    "sink-crash": sink_crash,
    "ingestor-crash": ingestor_crash,
    "broker-failure": broker_failure,
    "two-brokers-down": two_brokers_down,
}


def save(results: dict) -> None:
    RESULTS.parent.mkdir(exist_ok=True)
    stored = json.loads(RESULTS.read_text()) if RESULTS.exists() else {}
    stored.update(results)
    RESULTS.write_text(json.dumps(stored, indent=2, sort_keys=True) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenarios", nargs="*", help="default: all")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--no-save", action="store_true")
    args = parser.parse_args()

    if args.list:
        for name, function in SCENARIOS.items():
            print(f"{name:18} {function.__doc__.strip()}")
        return

    unknown = [name for name in args.scenarios if name not in SCENARIOS]
    if unknown:
        parser.error(f"unknown scenario: {', '.join(unknown)}")

    results = {}
    for name in args.scenarios or list(SCENARIOS):
        log.info("running %s", name)
        started = time.time()
        result = SCENARIOS[name]()
        result["ran_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        result["seconds"] = round(time.time() - started, 1)
        results[name] = result
        print(json.dumps({name: result}, indent=2))

    if not args.no_save:
        save(results)
    failed = [name for name, result in results.items() if not result["passed"]]
    if failed:
        log.error("failed: %s", ", ".join(failed))
        sys.exit(1)
    log.info("all %d scenarios passed", len(results))


if __name__ == "__main__":
    main()
