"""Keeps wiki.page-latest holding the latest edit for every page.

The topic is compacted and keyed by page, so it works as a table rather
than a log: Kafka keeps the newest record per page and removes older ones
in the background. A new consumer reads it from the beginning and gets the
current state of every page without replaying the full edit history.

Arrival order is not event order. wiki.raw is keyed by event id, so two
edits to one page usually sit on different raw partitions, and the cleaner
reads those in parallel. In real traffic an older edit reached wiki.clean
after a newer one about once per 550 records. Written blindly, it would
replace the newer edit, and an edit arriving after its page's deletion would
bring the page back. So every page remembers the event time last applied,
deletions included, and anything older is skipped.

That memory has to survive restarts, and the output topic already holds it:
compacted, it is the latest state per page. wiki.clean and wiki.page-latest
are keyed the same way with the same partition count, so the owner of an
input partition rebuilds its guard by reading the matching output partition.
Kafka Streams backs its state stores with changelog topics the same way.
"""

import os
import signal
import sys
from pathlib import Path
from typing import Dict, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import (  # noqa: E402
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    KafkaException,
    TopicPartition,
)

from common.config import TOPIC_CLEAN, TOPIC_PAGE_LATEST, consumer_config, scoped  # noqa: E402
from common.metrics import setup_logging  # noqa: E402
from common.models import CleanEvent  # noqa: E402
from processors.page_latest import PageLatest, event_ms, page_update  # noqa: E402
from processors.transactional import TransactionalProcessor  # noqa: E402

log = setup_logging("page-state")

GROUP_ID = scoped("page-state")
INSTANCE = os.getenv("PAGE_STATE_INSTANCE", "1")
LOAD_POLL_SEC = 5.0


def transactional_id(instance: str) -> str:
    return scoped(f"page-state-{instance}")


def remember(applied: Dict[str, int], key: str, time_ms: int) -> None:
    """Applied times only move forward."""
    if time_ms > applied.get(key, -1):
        applied[key] = time_ms


def is_stale(applied: Dict[str, int], key: str, time_ms: int) -> bool:
    """Older than what the page already shows. Equal times still apply."""
    return time_ms < applied.get(key, -1)


def time_of_record(value: Optional[bytes], record_timestamp: int) -> int:
    """A record carries its event time; a tombstone only has the record timestamp."""
    if value is not None:
        try:
            return event_ms(PageLatest.from_json(value).event_time)
        except (ValueError, TypeError, KeyError):
            pass
    return record_timestamp


class PageState(TransactionalProcessor):
    name = "page state"
    input_topic = TOPIC_CLEAN
    group_id = GROUP_ID

    def __init__(self, instance: str = INSTANCE):
        super().__init__(transactional_id(instance), log, "page-state")
        self.applied: Dict[int, Dict[str, int]] = {}
        self.updates = 0
        self.tombstones = 0
        self.stale = 0
        self.ignored = 0

    def partitions_assigned(self, consumer, partitions) -> None:
        for tp in partitions:
            self.applied[tp.partition] = self.load_applied(tp.partition)
            log.info(
                "partition %d: guard rebuilt from %d pages in %s",
                tp.partition,
                len(self.applied[tp.partition]),
                TOPIC_PAGE_LATEST,
            )

    def partitions_revoked(self, partitions) -> None:
        for tp in partitions:
            self.applied.pop(tp.partition, None)

    def load_applied(self, partition: int) -> Dict[str, int]:
        """Read the whole output partition: compacted, it is small."""
        consumer = Consumer(
            consumer_config(
                scoped("page-state-loader"),
                **{"group.id": scoped("page-state-loader"), "enable.partition.eof": True},
            )
        )
        applied: Dict[str, int] = {}
        try:
            consumer.assign([TopicPartition(TOPIC_PAGE_LATEST, partition, OFFSET_BEGINNING)])
            while True:
                msg = consumer.poll(LOAD_POLL_SEC)
                if msg is None:
                    break
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    raise KafkaException(msg.error())
                if msg.key() is None:
                    continue
                _, timestamp = msg.timestamp()
                remember(applied, msg.key().decode("utf-8"), time_of_record(msg.value(), timestamp))
        finally:
            consumer.close()
        return applied

    def handle(self, msg) -> None:
        try:
            event = CleanEvent.from_json(msg.value())
        except (ValueError, TypeError) as exc:
            log.error("unreadable record at offset %d: %s", msg.offset(), exc)
            self.meter.mark_error()
            return

        update = page_update(event)
        if update is None:
            self.ignored += 1
            return

        key, record = update
        time_ms = event_ms(event.event_time)
        applied = self.applied.setdefault(msg.partition(), {})
        if is_stale(applied, key, time_ms):
            self.stale += 1
            return

        # The record timestamp is the event time, so a tombstone, which has
        # no value, still says when the page was deleted.
        self.producer.produce(
            TOPIC_PAGE_LATEST,
            key=key.encode("utf-8"),
            value=record.to_json() if record is not None else None,
            timestamp=time_ms,
        )
        remember(applied, key, time_ms)
        if record is None:
            self.tombstones += 1
            log.info("page deleted, tombstone written for %s", key)
        else:
            self.updates += 1
        self.meter.mark()

    def summary(self) -> str:
        return (
            f"{self.updates} page updates, {self.tombstones} tombstones, "
            f"{self.stale} older than the page's latest skipped, "
            f"{self.ignored} events not affecting page content"
        )


def main() -> None:
    instance = sys.argv[1] if len(sys.argv) > 1 else INSTANCE
    processor = PageState(instance)
    signal.signal(signal.SIGINT, processor.stop)
    signal.signal(signal.SIGTERM, processor.stop)
    processor.run()


if __name__ == "__main__":
    main()
