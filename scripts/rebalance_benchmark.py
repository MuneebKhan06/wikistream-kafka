"""Measures how long processing pauses when a consumer joins a group.

A steady stream goes into a six partition topic. One consumer reads it,
then a second joins, then a third. Every consumer records when it received
each message, so for each partition the pause caused by a join is the
longest gap between two consecutive messages around it.

Run once with an eager assignor, where every member gives up every
partition on each rebalance, and once with cooperative-sticky, where only
the partitions that change owner stop.

The heartbeat interval matters as much as the assignor. Members learn that
a rebalance has started from a heartbeat response, and a cooperative
rebalance takes two rounds, so a partition changing owner waits about one
heartbeat interval between being revoked and being handed over.

Usage:
  python scripts/rebalance_benchmark.py                 # both, saved to benchmarks/
  python scripts/rebalance_benchmark.py --assignors range
  python scripts/rebalance_benchmark.py --heartbeat-ms 3000 1000 500
"""

import argparse
import json
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.config import PROJECT_ROOT  # noqa: E402

RESULTS = PROJECT_ROOT / "benchmarks" / "rebalance.json"
PARTITIONS = 6
RATE = 3000
SETTLE_SEC = 10
WINDOW_BEFORE_SEC = 1.0
WINDOW_AFTER_SEC = 9.0
STOPPED_SEC = 0.5


def worker(group: str, topic: str, assignor: str, out_path: str, heartbeat_ms: str) -> None:
    """One group member. Logs every receive time and every rebalance callback."""
    from confluent_kafka import Consumer, KafkaError

    from common.config import BOOTSTRAP_SERVERS

    cooperative = assignor.startswith("cooperative")
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": group,
            "partition.assignment.strategy": assignor,
            "heartbeat.interval.ms": int(heartbeat_ms),
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 500,
            # Deliver messages as they arrive, so gaps reflect pauses rather
            # than fetch batching.
            "fetch.wait.max.ms": 10,
        }
    )
    out = open(out_path, "w", buffering=1)
    running = {"on": True}
    signal.signal(signal.SIGTERM, lambda *_: running.update(on=False))

    def on_assign(c, parts):
        out.write(f"A {time.time():.4f} {','.join(str(p.partition) for p in parts)}\n")
        if cooperative:
            c.incremental_assign(parts)
        else:
            c.assign(parts)

    def on_revoke(c, parts):
        out.write(f"R {time.time():.4f} {','.join(str(p.partition) for p in parts)}\n")
        if cooperative:
            c.incremental_unassign(parts)
        else:
            c.unassign()

    consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
    while running["on"]:
        msg = consumer.poll(0.05)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF and msg.error().fatal():
                break
            continue
        out.write(f"M {time.time():.4f} {msg.partition()}\n")
    consumer.close()
    out.close()


def parse(path: Path):
    received = defaultdict(list)
    callbacks = []
    for line in path.read_text().splitlines():
        kind, stamp, rest = (line.split(" ", 2) + [""])[:3]
        if kind == "M":
            received[int(rest)].append(float(stamp))
        else:
            parts = [int(p) for p in rest.split(",") if p]
            callbacks.append((kind, float(stamp), parts))
    return received, callbacks


def longest_gap(times: list, start: float, end: float) -> float:
    """Longest stretch inside [start, end] with no message received."""
    inside = [t for t in times if start <= t <= end]
    edges = [start] + inside + [end]
    return max(b - a for a, b in zip(edges, edges[1:]))


def owner_at(per_worker: dict, partition: int, when: float):
    """Which worker received this partition's messages just before `when`."""
    best = (None, -1.0)
    for name, received in per_worker.items():
        earlier = [t for t in received.get(partition, []) if t <= when]
        if earlier and earlier[-1] > best[1]:
            best = (name, earlier[-1])
    return best[0]


def analyse(per_worker: dict, joins: dict) -> dict:
    merged = defaultdict(list)
    for received in per_worker.values():
        for partition, times in received.items():
            merged[partition].extend(times)
    for times in merged.values():
        times.sort()

    result = {}
    for label, joined_at in joins.items():
        start, end = joined_at - WINDOW_BEFORE_SEC, joined_at + WINDOW_AFTER_SEC
        pauses = {p: longest_gap(merged[p], start, end) for p in range(PARTITIONS)}
        moved = [
            p
            for p in range(PARTITIONS)
            if owner_at(per_worker, p, start) != owner_at(per_worker, p, end)
        ]
        stopped = [p for p, gap in pauses.items() if gap >= STOPPED_SEC]
        result[label] = {
            "partitions_that_changed_owner": len(moved),
            "partitions_that_stopped": len(stopped),
            "longest_pause_sec": round(max(pauses.values()), 3),
            "mean_pause_sec": round(sum(pauses.values()) / PARTITIONS, 3),
            "pause_per_partition_sec": {str(p): round(g, 3) for p, g in sorted(pauses.items())},
            "partition_seconds_lost": round(sum(g for g in pauses.values() if g >= STOPPED_SEC), 2),
        }
    return result


def run(assignor: str, heartbeat_ms: int) -> dict:
    from confluent_kafka.admin import AdminClient, NewTopic

    from common.config import REPLICATION_FACTOR, admin_config
    from common.sandbox import Sandbox

    with Sandbox("rb", database=False) as box:
        topic = box.topic("bench")
        # Held in a variable: a temporary client is collected before its
        # request completes.
        admin = AdminClient(admin_config())
        admin.create_topics([NewTopic(topic, PARTITIONS, REPLICATION_FACTOR)])[topic].result()
        time.sleep(3)
        box.start(
            "scripts/replay.py",
            "--topic", topic, "--rate", str(RATE), "--loops", "100000",
            name="producer",
        )
        time.sleep(3)

        group = box.group("bench")
        files, joins = {}, {}
        for number in (1, 2, 3):
            name = f"consumer-{number}"
            files[name] = box.logs / f"{name}.times"
            if number > 1:
                joins[f"consumer {number} joins"] = time.time()
            box.start(
                "scripts/rebalance_benchmark.py",
                "--worker", group, topic, assignor, str(files[name]), str(heartbeat_ms),
                name=name,
            )
            time.sleep(SETTLE_SEC + (2 if number == 1 else 0))

        for name in files:
            box.stop(name)
        box.stop("producer")

        per_worker, callbacks = {}, {}
        for name, path in files.items():
            per_worker[name], callbacks[name] = parse(path)

        quiet_start = min(joins.values()) - 6
        merged = defaultdict(list)
        for received in per_worker.values():
            for partition, times in received.items():
                merged[partition].extend(times)
        baseline = max(
            longest_gap(sorted(merged[p]), quiet_start, quiet_start + 4) for p in range(PARTITIONS)
        )
        return {
            "assignor": assignor,
            "heartbeat_interval_ms": heartbeat_ms,
            "produce_rate_per_sec": RATE,
            "partitions": PARTITIONS,
            "steady_state_longest_gap_sec": round(baseline, 3),
            "messages_received": sum(len(t) for t in merged.values()),
            "joins": analyse(per_worker, joins),
            "revocations_per_consumer": {
                name: sum(len(parts) for kind, _, parts in events if kind == "R")
                for name, events in callbacks.items()
            },
        }


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker(*sys.argv[2:7])
        return

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--assignors", nargs="+", default=["range", "cooperative-sticky"])
    parser.add_argument(
        "--heartbeat-ms",
        nargs="+",
        type=int,
        default=[3000],
        help="heartbeat intervals to measure; 3000 is the client default",
    )
    parser.add_argument("--no-save", action="store_true")
    args = parser.parse_args()

    results = {}
    for assignor in args.assignors:
        for heartbeat_ms in args.heartbeat_ms:
            label = f"{assignor} @ {heartbeat_ms} ms heartbeat"
            print(f"measuring {label} ...", flush=True)
            results[label] = run(assignor, heartbeat_ms)
            results[label]["ran_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            summary = {
                join: (data["partitions_that_stopped"], data["longest_pause_sec"])
                for join, data in results[label]["joins"].items()
            }
            print(f"  (partitions stopped, longest pause in seconds): {summary}", flush=True)

    if not args.no_save:
        RESULTS.parent.mkdir(exist_ok=True)
        stored = json.loads(RESULTS.read_text()) if RESULTS.exists() else {}
        stored.update(results)
        RESULTS.write_text(json.dumps(stored, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
