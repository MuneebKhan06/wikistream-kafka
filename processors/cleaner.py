"""Reads wiki.raw, deduplicates and cleans, writes wiki.clean.

Each instance needs its own transactional id: Kafka fences a transactional
id to one producer at a time, so a second instance reusing it would kill
the first. Set CLEANER_INSTANCE or pass it as the first argument.

Output messages and the input offsets are committed in one Kafka
transaction, so a crash mid batch leaves neither half behind: the
transaction is aborted, read_committed consumers never see its output, and
processing restarts from the last committed offset.

Dedup state is per partition and lives in memory. A partition that moves to
another instance takes its state with it, so revoked partitions are cleared
on rebalance rather than kept around stale.
"""

import os
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer  # noqa: E402

from common.config import (  # noqa: E402
    TOPIC_CLEAN,
    TOPIC_DLQ,
    TOPIC_RAW,
    consumer_config,
    transactional_producer_config,
)
from common.metrics import RateMeter, log_lag, setup_logging  # noqa: E402
from processors.dedup import DedupCache  # noqa: E402
from processors.transform import Clean, Duplicate, Rejected, transform  # noqa: E402
from processors.warmup import CacheWarmer  # noqa: E402

log = setup_logging("cleaner")

GROUP_ID = "cleaner"
INSTANCE = os.getenv("CLEANER_INSTANCE", "1")
COMMIT_INTERVAL_SEC = 0.5
MAX_BATCH = 2000
LAG_INTERVAL_SEC = 30.0


def transactional_id(instance: str) -> str:
    return f"cleaner-{instance}"


class Cleaner:
    def __init__(self, instance: str = INSTANCE, warmer: CacheWarmer = None):
        self.transactional_id = transactional_id(instance)
        self.producer = Producer(transactional_producer_config(self.transactional_id))
        self.consumer = Consumer(consumer_config(GROUP_ID))
        self.warmer = warmer if warmer is not None else CacheWarmer()
        self.caches = {}
        self.meter = RateMeter(log, "clean")
        self.running = True
        self.in_transaction = False
        self.produced = 0
        self.duplicates = 0
        self.rejected = 0
        self._last_lag_report = time.monotonic()

    def stop(self, *_):
        log.info("stop requested, finishing current transaction")
        self.running = False

    def on_assign(self, consumer, partitions):
        consumer.incremental_assign(partitions)
        log.info("assigned partitions: %s", sorted(p.partition for p in partitions))
        self.warm(consumer, partitions)

    def warm(self, consumer, partitions) -> None:
        """Rebuild dedup state for new partitions before processing them.

        The replay stops at each partition's committed offset, which is also
        where processing resumes, so events the group has not yet committed
        stay unknown to the cache and are cleaned normally.
        """
        if not partitions:
            return
        committed = {
            tp.partition: tp.offset
            for tp in consumer.committed(list(partitions), timeout=30)
        }
        for tp in partitions:
            cache = self.caches.setdefault(tp.partition, DedupCache())
            offset = committed.get(tp.partition, -1)
            if offset is None or offset < 0:
                log.info(
                    "partition %d has no committed offset, skipping warmup",
                    tp.partition,
                )
                continue
            result = self.warmer.warm(tp.partition, cache, offset)
            if result.replayed:
                log.info(
                    "warmed partition %d with %d ids from offsets %d to %d in %.1fs",
                    tp.partition,
                    result.loaded,
                    result.from_offset,
                    result.until_offset - 1,
                    result.seconds,
                )

    def on_revoke(self, consumer, partitions):
        """A revoked partition may be processed elsewhere, so drop its state."""
        if self.in_transaction:
            self._abort()
        for tp in partitions:
            cache = self.caches.pop(tp.partition, None)
            if cache:
                cache.clear()
        consumer.incremental_unassign(partitions)
        log.info("revoked partitions: %s", sorted(p.partition for p in partitions))

    def cache_for(self, partition: int) -> DedupCache:
        return self.caches.setdefault(partition, DedupCache())

    def _abort(self) -> None:
        try:
            self.producer.abort_transaction()
        except KafkaException as exc:
            log.warning("abort failed: %s", exc)
        self.in_transaction = False

    def handle(self, msg) -> None:
        cache = self.cache_for(msg.partition())
        _, message_time = msg.timestamp()
        result = transform(msg.value(), cache, message_time_ms=message_time)

        if isinstance(result, Clean):
            self.producer.produce(
                TOPIC_CLEAN,
                key=result.key.encode("utf-8"),
                value=result.event.to_json(),
                timestamp=message_time if message_time > 0 else 0,
            )
            self.produced += 1
            self.meter.mark()
        elif isinstance(result, Duplicate):
            self.duplicates += 1
        elif isinstance(result, Rejected):
            self.rejected += 1
            self.meter.mark_error()
            log.warning("rejected offset %d: %s", msg.offset(), result.reason)
            self.producer.produce(
                TOPIC_DLQ,
                value=result.envelope(msg.topic(), msg.partition(), msg.offset()),
            )

    def collect_batch(self) -> list:
        """Collect messages until the batch is full or the interval is up."""
        deadline = time.monotonic() + COMMIT_INTERVAL_SEC
        batch = []
        while self.running and len(batch) < MAX_BATCH:
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

    def process_batch(self, batch: list) -> None:
        self.producer.begin_transaction()
        self.in_transaction = True
        try:
            for msg in batch:
                self.handle(msg)
            positions = self.consumer.position(self.consumer.assignment())
            self.producer.send_offsets_to_transaction(
                positions, self.consumer.consumer_group_metadata()
            )
            self.producer.commit_transaction()
            self.in_transaction = False
        except KafkaException as exc:
            error = exc.args[0]
            if error.txn_requires_abort():
                log.warning("aborting transaction: %s", error)
                self._abort()
            else:
                raise

    def _report_lag(self) -> None:
        now = time.monotonic()
        if now - self._last_lag_report >= LAG_INTERVAL_SEC:
            log_lag(log, self.consumer, self.consumer.assignment())
            self._last_lag_report = now

    def run(self) -> None:
        self.producer.init_transactions()
        self.consumer.subscribe(
            [TOPIC_RAW], on_assign=self.on_assign, on_revoke=self.on_revoke
        )
        log.info("cleaner started, transactional id %s", self.transactional_id)

        while self.running:
            batch = self.collect_batch()
            if batch:
                self.process_batch(batch)
            self._report_lag()

        self.shutdown()

    def shutdown(self) -> None:
        if self.in_transaction:
            self._abort()
        self.consumer.close()
        self.meter.report(force=True)
        log.info(
            "stopped, %d cleaned, %d duplicates dropped, %d rejected",
            self.produced,
            self.duplicates,
            self.rejected,
        )


def main() -> None:
    instance = sys.argv[1] if len(sys.argv) > 1 else INSTANCE
    cleaner = Cleaner(instance)
    signal.signal(signal.SIGINT, cleaner.stop)
    signal.signal(signal.SIGTERM, cleaner.stop)
    cleaner.run()


if __name__ == "__main__":
    main()
