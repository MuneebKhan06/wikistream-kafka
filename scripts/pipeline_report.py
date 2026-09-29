"""Measures end to end latency and how evenly traffic spreads across partitions.

Latency is from the moment an edit happened on the wiki to the moment its
row was committed in PostgreSQL, so it covers the whole pipeline plus the
source's own delay. While the ingestor is catching up on backlog this is
dominated by how far behind it started, so measure it with the ingestor
reading live.

Partition balance is read from wiki.clean. Every edit to a page shares one
key and so one partition, which keeps per page state local but means one
busy page loads one partition. For each minute of traffic this finds the
partition carrying the largest share, and the page behind it.

Usage:
  python scripts/pipeline_report.py                  # both reports
  python scripts/pipeline_report.py --latency --minutes 10
  python scripts/pipeline_report.py --partitions --min-events 500
"""

import argparse
import sys
import uuid
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Consumer, KafkaError, KafkaException  # noqa: E402

from common.config import TOPIC_CLEAN, consumer_config  # noqa: E402
from common.metrics import setup_logging  # noqa: E402
from sinks.db import connect  # noqa: E402

log = setup_logging("pipeline-report")

MINUTE_MS = 60_000
IDLE_TIMEOUT_SEC = 10.0

LATENCY = """
    SELECT
        COUNT(*),
        percentile_cont(0.50) WITHIN GROUP (ORDER BY lag),
        percentile_cont(0.95) WITHIN GROUP (ORDER BY lag),
        percentile_cont(0.99) WITHIN GROUP (ORDER BY lag),
        MAX(lag)
    FROM (
        SELECT EXTRACT(EPOCH FROM ingested_at - event_time) * 1000 AS lag
        FROM edits
        WHERE ingested_at > NOW() - (%(minutes)s * INTERVAL '1 minute')
    ) AS recent
"""


def latency_report(minutes: float) -> dict:
    connection = connect()
    try:
        with connection.cursor() as cur:
            cur.execute(LATENCY, {"minutes": minutes})
            count, p50, p95, p99, worst = cur.fetchone()
    finally:
        connection.close()
    return {"rows": count, "p50": p50, "p95": p95, "p99": p99, "max": worst}


def scan_partitions(topic: str = TOPIC_CLEAN) -> dict:
    """Count records per (minute, partition) and the busiest key in each."""
    consumer = Consumer(
        consumer_config(
            "pipeline-report",
            **{
                "group.id": f"pipeline-report-{uuid.uuid4().hex[:8]}",
                "enable.partition.eof": True,
            },
        )
    )
    consumer.subscribe([topic])
    partitions = len(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
    per_minute = defaultdict(Counter)
    keys = defaultdict(Counter)
    totals = Counter()
    finished = set()

    try:
        while len(finished) < partitions:
            msg = consumer.poll(IDLE_TIMEOUT_SEC)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    finished.add(msg.partition())
                    continue
                raise KafkaException(msg.error())
            _, timestamp = msg.timestamp()
            minute = timestamp // MINUTE_MS
            per_minute[minute][msg.partition()] += 1
            keys[(minute, msg.partition())][msg.key()] += 1
            totals[msg.partition()] += 1
    finally:
        consumer.close()

    return {
        "partitions": partitions,
        "per_minute": per_minute,
        "keys": keys,
        "totals": totals,
    }


def hottest_minutes(scan: dict, min_events: int, limit: int) -> list:
    """Minutes ranked by the largest single partition share."""
    rows = []
    for minute, counts in scan["per_minute"].items():
        total = sum(counts.values())
        if total < min_events:
            continue
        partition, top = counts.most_common(1)[0]
        key, key_count = scan["keys"][(minute, partition)].most_common(1)[0]
        rows.append(
            {
                "minute": minute,
                "events": total,
                "partition": partition,
                "share": top / total,
                "key": key.decode("utf-8", "replace") if key else "",
                "key_share": key_count / total,
            }
        )
    rows.sort(key=lambda r: -r["share"])
    return rows[:limit]


def fmt_ms(value) -> str:
    if value is None:
        return "n/a"
    if value >= 60_000:
        return f"{value / 60_000:.1f} min"
    if value >= 1000:
        return f"{value / 1000:.2f} s"
    return f"{value:.0f} ms"


def print_latency(report: dict, minutes: float) -> None:
    print(f"end to end latency, rows stored in the last {minutes:g} minutes")
    if not report["rows"]:
        print("  no rows stored in that window")
        return
    print(f"  rows {report['rows']}")
    for label in ("p50", "p95", "p99", "max"):
        print(f"  {label:<4} {fmt_ms(report[label])}")


def print_partitions(scan: dict, min_events: int, limit: int) -> None:
    from datetime import datetime, timezone

    total = sum(scan["totals"].values())
    fair = 1 / scan["partitions"] if scan["partitions"] else 0
    print(f"partition balance on {TOPIC_CLEAN}, {total} records")
    for partition in range(scan["partitions"]):
        share = scan["totals"][partition] / total if total else 0
        print(f"  p{partition}  {scan['totals'][partition]:>8}  {share:6.1%}")
    print(f"  an even spread would be {fair:.1%} each")
    print()
    print(f"minutes with the largest single partition share (at least {min_events} events)")
    for row in hottest_minutes(scan, min_events, limit):
        when = datetime.fromtimestamp(row["minute"] * 60, tz=timezone.utc)
        print(
            f"  {when:%Y-%m-%d %H:%M}  p{row['partition']} carried {row['share']:.1%} "
            f"of {row['events']} events; top page {row['key'][:50]} "
            f"({row['key_share']:.1%} of the minute)"
        )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--latency", action="store_true")
    parser.add_argument("--partitions", action="store_true")
    parser.add_argument("--minutes", type=float, default=15.0)
    parser.add_argument("--min-events", type=int, default=200)
    parser.add_argument("--limit", type=int, default=5)
    args = parser.parse_args(argv)
    if not args.latency and not args.partitions:
        args.latency = args.partitions = True
    return args


def main() -> None:
    args = parse_args()
    if args.latency:
        print_latency(latency_report(args.minutes), args.minutes)
        print()
    if args.partitions:
        print_partitions(scan_partitions(), args.min_events, args.limit)


if __name__ == "__main__":
    main()
