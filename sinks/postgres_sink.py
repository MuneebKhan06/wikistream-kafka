"""Writes wiki.clean into the edits table in PostgreSQL.

Offsets are committed only after PostgreSQL has committed the rows, so a
crash in between replays the batch instead of losing it. The insert is an
upsert on event_id, which makes that replay a no-op and gives exactly-once
results in the database without a distributed transaction.
"""

import signal
import sys
import time
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Consumer, KafkaError, KafkaException  # noqa: E402

from common.config import TOPIC_CLEAN, consumer_config, scoped  # noqa: E402
from common.metrics import RateMeter, log_lag, setup_logging  # noqa: E402
from common.models import CleanEvent  # noqa: E402
from common.offsets import report_retention_gaps  # noqa: E402
from sinks.db import insert_edits, wait_for_database  # noqa: E402

log = setup_logging("postgres-sink")

GROUP_ID = scoped("storage")
BATCH_SIZE = 500
BATCH_INTERVAL_SEC = 1.0
LAG_INTERVAL_SEC = 30.0


class PostgresSink:
    """Topic to table: decode a batch, write it, then commit its offsets.

    Parameterised so every table shares the same rows first, offsets second
    ordering. `write` must be idempotent and return the rows it inserted.
    """

    def __init__(
        self,
        topic: str = TOPIC_CLEAN,
        group_id: str = GROUP_ID,
        decode: Callable = CleanEvent.from_json,
        write: Callable = insert_edits,
        label: str = "sink",
    ):
        self.topic = topic
        self.group_id = group_id
        self.decode = decode
        self.write = write
        self.consumer = Consumer(consumer_config(group_id))
        self.connection = wait_for_database(log=log)
        self.meter = RateMeter(log, label)
        self.running = True
        self.inserted = 0
        self.skipped = 0
        self._last_lag_report = time.monotonic()

    def stop(self, *_):
        log.info("stop requested, finishing current batch")
        self.running = False

    def on_assign(self, consumer, partitions):
        consumer.incremental_assign(partitions)
        log.info("assigned partitions: %s", sorted(p.partition for p in partitions))
        report_retention_gaps(log, consumer, partitions)

    def on_revoke(self, consumer, partitions):
        consumer.incremental_unassign(partitions)
        log.info("revoked partitions: %s", sorted(p.partition for p in partitions))

    def collect_batch(self) -> list:
        deadline = time.monotonic() + BATCH_INTERVAL_SEC
        batch = []
        while self.running and len(batch) < BATCH_SIZE:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            msg = self.consumer.poll(remaining)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            batch.append(msg)
        return batch

    def write_batch(self, batch: list) -> None:
        """Rows first, offsets second. The order is the whole guarantee."""
        events = []
        for msg in batch:
            try:
                events.append(self.decode(msg.value()))
            except (ValueError, TypeError, KeyError) as exc:
                self.meter.mark_error()
                log.error(
                    "unreadable record at %s[%d]@%d: %s",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    exc,
                )

        written = self.write(self.connection, events)
        self.inserted += written
        self.skipped += len(events) - written
        self.meter.mark(len(events))
        self.consumer.commit(asynchronous=False)

    def _report_lag(self) -> None:
        now = time.monotonic()
        if now - self._last_lag_report >= LAG_INTERVAL_SEC:
            log_lag(log, self.consumer, self.consumer.assignment())
            self._last_lag_report = now

    def run(self) -> None:
        self.consumer.subscribe(
            [self.topic], on_assign=self.on_assign, on_revoke=self.on_revoke
        )
        log.info("postgres sink started, %s into group %s", self.topic, self.group_id)

        while self.running:
            batch = self.collect_batch()
            if batch:
                self.write_batch(batch)
            self._report_lag()

        self.shutdown()

    def shutdown(self) -> None:
        self.consumer.close()
        self.connection.close()
        self.meter.report(force=True)
        log.info(
            "stopped, %d rows inserted, %d already present",
            self.inserted,
            self.skipped,
        )


def main() -> None:
    sink = PostgresSink()
    signal.signal(signal.SIGINT, sink.stop)
    signal.signal(signal.SIGTERM, sink.stop)
    sink.run()


if __name__ == "__main__":
    main()
