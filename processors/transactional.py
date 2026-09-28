"""Shared loop for processors that consume, transform and produce exactly once.

Every batch runs inside one Kafka transaction: the output messages and the
consumer offsets for the input that produced them commit together or not at
all. A crash mid batch aborts the transaction, read_committed consumers never
see its output, and the next owner resumes from the last committed offset.

Subclasses supply what differs: the input topic and group, how one message is
handled, and what state to build or drop as partitions move between
instances. Each instance needs its own transactional id, because Kafka
fences an id to one producer at a time and a second instance reusing it
would stop the first.
"""

import logging
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from common.config import consumer_config, transactional_producer_config
from common.metrics import RateMeter, log_lag

COMMIT_INTERVAL_SEC = 0.5
MAX_BATCH = 2000
LAG_INTERVAL_SEC = 30.0


class TransactionalProcessor:
    name = "processor"
    input_topic: str = ""
    group_id: str = ""
    commit_interval_sec = COMMIT_INTERVAL_SEC
    max_batch = MAX_BATCH
    lag_interval_sec = LAG_INTERVAL_SEC

    def __init__(self, transactional_id: str, log: logging.Logger, meter_label: str):
        self.transactional_id = transactional_id
        self.log = log
        self.producer = Producer(transactional_producer_config(transactional_id))
        self.consumer = Consumer(consumer_config(self.group_id))
        self.meter = RateMeter(log, meter_label)
        self.running = True
        self.in_transaction = False
        self._last_lag_report = time.monotonic()

    # Hooks for subclasses.

    def handle(self, msg) -> None:
        """Process one input message, producing any output with self.producer."""
        raise NotImplementedError

    def partitions_assigned(self, consumer, partitions) -> None:
        """Build state for newly owned partitions before they are processed."""

    def partitions_revoked(self, partitions) -> None:
        """Drop state for partitions another instance now owns."""

    def periodic(self) -> None:
        """Called once per loop, for housekeeping such as expiring state."""

    def summary(self) -> str:
        return ""

    # The loop itself.

    def stop(self, *_):
        self.log.info("stop requested, finishing current transaction")
        self.running = False

    def on_assign(self, consumer, partitions):
        consumer.incremental_assign(partitions)
        self.log.info("assigned partitions: %s", sorted(p.partition for p in partitions))
        self.partitions_assigned(consumer, partitions)

    def on_revoke(self, consumer, partitions):
        # Output for a revoked partition must not commit after it moves, so an
        # open transaction is aborted before the state for it is dropped.
        if self.in_transaction:
            self._abort()
        self.partitions_revoked(partitions)
        consumer.incremental_unassign(partitions)
        self.log.info("revoked partitions: %s", sorted(p.partition for p in partitions))

    def _abort(self) -> None:
        try:
            self.producer.abort_transaction()
        except KafkaException as exc:
            self.log.warning("abort failed: %s", exc)
        self.in_transaction = False

    def collect_batch(self) -> list:
        """Collect messages until the batch is full or the interval is up."""
        deadline = time.monotonic() + self.commit_interval_sec
        batch = []
        while self.running and len(batch) < self.max_batch:
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
                self.log.warning("aborting transaction: %s", error)
                self._abort()
            else:
                raise

    def _report_lag(self) -> None:
        now = time.monotonic()
        if now - self._last_lag_report >= self.lag_interval_sec:
            log_lag(self.log, self.consumer, self.consumer.assignment())
            self._last_lag_report = now

    def run(self) -> None:
        self.producer.init_transactions()
        self.consumer.subscribe(
            [self.input_topic], on_assign=self.on_assign, on_revoke=self.on_revoke
        )
        self.log.info("%s started, transactional id %s", self.name, self.transactional_id)

        while self.running:
            batch = self.collect_batch()
            if batch:
                self.process_batch(batch)
            self.periodic()
            self._report_lag()

        self.shutdown()

    def shutdown(self) -> None:
        if self.in_transaction:
            self._abort()
        self.consumer.close()
        self.meter.report(force=True)
        summary = self.summary()
        self.log.info("stopped%s", f", {summary}" if summary else "")
