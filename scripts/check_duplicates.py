"""Reads a topic end to end and reports duplicate ids and per partition counts.

Used after the crash tests: if the cleaner's transactions hold, wiki.clean
contains every event id exactly once.

Usage: python scripts/check_duplicates.py [topic] [field]
"""

import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Consumer, KafkaError, KafkaException  # noqa: E402

from common.config import TOPIC_CLEAN, consumer_config  # noqa: E402
from common.metrics import setup_logging  # noqa: E402

log = setup_logging("check-duplicates")

IDLE_TIMEOUT_SEC = 10.0


def scan(topic: str, field: str = "event_id") -> dict:
    consumer = Consumer(
        consumer_config(
            "duplicate-check",
            **{"group.id": f"duplicate-check-{topic}", "enable.partition.eof": True},
        )
    )
    consumer.subscribe([topic])

    ids = Counter()
    per_partition = Counter()
    unreadable = 0
    finished = set()
    metadata = consumer.list_topics(topic, timeout=10)
    partition_count = len(metadata.topics[topic].partitions)

    try:
        while len(finished) < partition_count:
            msg = consumer.poll(IDLE_TIMEOUT_SEC)
            if msg is None:
                log.warning("no more messages, stopping early")
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    finished.add(msg.partition())
                    continue
                raise KafkaException(msg.error())

            per_partition[msg.partition()] += 1
            try:
                ids[json.loads(msg.value())[field]] += 1
            except (json.JSONDecodeError, KeyError, TypeError):
                unreadable += 1
    finally:
        consumer.close()

    duplicates = {key: count for key, count in ids.items() if count > 1}
    return {
        "records": sum(per_partition.values()),
        "unique_ids": len(ids),
        "duplicates": duplicates,
        "per_partition": dict(sorted(per_partition.items())),
        "unreadable": unreadable,
    }


def main() -> None:
    topic = sys.argv[1] if len(sys.argv) > 1 else TOPIC_CLEAN
    field = sys.argv[2] if len(sys.argv) > 2 else "event_id"
    result = scan(topic, field)

    log.info(
        "topic %s: %d records, %d unique %s",
        topic,
        result["records"],
        result["unique_ids"],
        field,
    )
    log.info("per partition: %s", result["per_partition"])
    if result["unreadable"]:
        log.warning("%d records could not be read", result["unreadable"])

    if result["duplicates"]:
        worst = sorted(result["duplicates"].items(), key=lambda kv: -kv[1])[:5]
        log.error("%d duplicate ids, worst: %s", len(result["duplicates"]), worst)
        sys.exit(1)
    log.info("no duplicates")


if __name__ == "__main__":
    main()
