"""Reads wiki.clean, detects edit wars, writes alerts to wiki.alerts.

Alerts and input offsets are committed in one Kafka transaction, so a crash
can neither lose an alert nor emit it twice. Alert ids are derived from the
events that triggered them, so even a replay that re-detects the same war
produces the same id and downstream writes stay idempotent.

wiki.clean is keyed by page, so every edit to a page arrives on one
partition in order. That lets each partition own an independent detector.
"""

import os
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer  # noqa: E402

from common.config import (  # noqa: E402
    TOPIC_ALERTS,
    TOPIC_CLEAN,
    consumer_config,
    transactional_producer_config,
)
from common.metrics import RateMeter, log_lag, setup_logging  # noqa: E402
from common.models import CleanEvent  # noqa: E402
from processors.edit_war_detector import EditWarDetector  # noqa: E402
from processors.transform import event_time_ms  # noqa: E402

log = setup_logging("edit-war")

GROUP_ID = "edit-wars"
INSTANCE = os.getenv("EDIT_WAR_INSTANCE", "1")
COMMIT_INTERVAL_SEC = 0.5
MAX_BATCH = 2000
EXPIRE_INTERVAL_SEC = 60.0
LAG_INTERVAL_SEC = 30.0


def transactional_id(instance: str) -> str:
    return f"edit-war-{instance}"


class EditWarProcessor:
    def __init__(self, instance: str = INSTANCE):
        self.transactional_id = transactional_id(instance)
        self.producer = Producer(transactional_producer_config(self.transactional_id))
        self.consumer = Consumer(consumer_config(GROUP_ID))
        self.detectors = {}
        self.meter = RateMeter(log, "edit-war")
        self.running = True
        self.in_transaction = False
        self.alerts = 0
        self._last_expire = time.monotonic()
        self._last_lag_report = time.monotonic()

    def stop(self, *_):
        log.info("stop requested, finishing current transaction")
        self.running = False

    def detector_for(self, partition: int) -> EditWarDetector:
        return self.detectors.setdefault(partition, EditWarDetector())

    def on_assign(self, consumer, partitions):
        consumer.incremental_assign(partitions)
        for tp in partitions:
            self.detector_for(tp.partition)
        log.info("assigned partitions: %s", sorted(p.partition for p in partitions))

    def on_revoke(self, consumer, partitions):
        if self.in_transaction:
            self._abort()
        for tp in partitions:
            self.detectors.pop(tp.partition, None)
        consumer.incremental_unassign(partitions)
        log.info("revoked partitions: %s", sorted(p.partition for p in partitions))

    def _abort(self) -> None:
        try:
            self.producer.abort_transaction()
        except KafkaException as exc:
            log.warning("abort failed: %s", exc)
        self.in_transaction = False

    def handle(self, msg) -> None:
        try:
            event = CleanEvent.from_json(msg.value())
        except (ValueError, TypeError) as exc:
            log.error("unreadable record at offset %d: %s", msg.offset(), exc)
            self.meter.mark_error()
            return

        _, message_time = msg.timestamp()
        detector = self.detector_for(msg.partition())
        alert = detector.observe(event, event_time_ms(event, message_time))
        self.meter.mark()
        if alert is None:
            return

        self.producer.produce(
            TOPIC_ALERTS,
            key=alert.page_key.encode("utf-8"),
            value=alert.to_json(),
        )
        self.alerts += 1
        log.info(
            "edit war on %s: %d reverts by %d users between %s and %s",
            alert.page_key,
            alert.revert_count,
            len(alert.users),
            alert.window_start[11:19],
            alert.window_end[11:19],
        )

    def collect_batch(self) -> list:
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

    def _periodic(self) -> None:
        now = time.monotonic()
        if now - self._last_expire >= EXPIRE_INTERVAL_SEC:
            expired = sum(d.expire() for d in self.detectors.values())
            if expired:
                log.debug("expired %d quiet pages", expired)
            self._last_expire = now
        if now - self._last_lag_report >= LAG_INTERVAL_SEC:
            log_lag(log, self.consumer, self.consumer.assignment())
            self._last_lag_report = now

    def run(self) -> None:
        self.producer.init_transactions()
        self.consumer.subscribe(
            [TOPIC_CLEAN], on_assign=self.on_assign, on_revoke=self.on_revoke
        )
        log.info("edit war detection started, transactional id %s", self.transactional_id)

        while self.running:
            batch = self.collect_batch()
            if batch:
                self.process_batch(batch)
            self._periodic()

        self.shutdown()

    def shutdown(self) -> None:
        if self.in_transaction:
            self._abort()
        self.consumer.close()
        self.meter.report(force=True)
        tracked = sum(len(d.pages) for d in self.detectors.values())
        log.info("stopped, %d alerts raised, %d pages tracked", self.alerts, tracked)


def main() -> None:
    instance = sys.argv[1] if len(sys.argv) > 1 else INSTANCE
    processor = EditWarProcessor(instance)
    signal.signal(signal.SIGINT, processor.stop)
    signal.signal(signal.SIGTERM, processor.stop)
    processor.run()


if __name__ == "__main__":
    main()
