"""Reads wiki.clean, detects edit wars, writes alerts to wiki.alerts.

Runs on the shared transactional loop, so an alert and the input offsets
that produced it commit together: a crash can neither lose an alert nor
emit it twice. Alert ids are derived from the events that triggered them,
so even a replay that re-detects the same war produces the same id and
downstream writes stay idempotent.

wiki.clean is keyed by page, so every edit to a page arrives on one
partition in order. That lets each partition own an independent detector.

A detector's revert history lives in memory, so a new owner of a partition
first replays the last window of it up to the committed offset. Without
that, a war with two reverts before a restart and one after would be
missed. Alerts found during the replay are discarded: they were committed
in the same transactions as the offsets being replayed up to.
"""

import os
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.config import TOPIC_ALERTS, TOPIC_CLEAN, scoped  # noqa: E402
from common.metrics import setup_logging  # noqa: E402
from common.models import CleanEvent  # noqa: E402
from processors.edit_war_detector import DEFAULT_WINDOW_MS, EditWarDetector  # noqa: E402
from processors.transactional import TransactionalProcessor  # noqa: E402
from processors.transform import event_time_ms  # noqa: E402
from processors.warmup import PartitionReplayer, consumer_factory_for  # noqa: E402

log = setup_logging("edit-war")

GROUP_ID = scoped("edit-wars")
INSTANCE = os.getenv("EDIT_WAR_INSTANCE", "1")
EXPIRE_INTERVAL_SEC = 60.0


def transactional_id(instance: str) -> str:
    return scoped(f"edit-war-{instance}")


class EditWarProcessor(TransactionalProcessor):
    name = "edit war detection"
    input_topic = TOPIC_CLEAN
    group_id = GROUP_ID

    def __init__(self, instance: str = INSTANCE):
        super().__init__(transactional_id(instance), log, "edit-war")
        self.replayer = PartitionReplayer(
            TOPIC_CLEAN,
            DEFAULT_WINDOW_MS,
            consumer_factory=consumer_factory_for(scoped("edit-war-warmup")),
        )
        self.detectors = {}
        self.alerts = 0
        self._last_expire = time.monotonic()

    def detector_for(self, partition: int) -> EditWarDetector:
        return self.detectors.setdefault(partition, EditWarDetector())

    def partitions_assigned(self, consumer, partitions) -> None:
        self.warm(consumer, partitions)

    def warm(self, consumer, partitions) -> None:
        if not partitions:
            return
        committed = {
            tp.partition: tp.offset
            for tp in consumer.committed(list(partitions), timeout=30)
        }
        for tp in partitions:
            detector = self.detectors[tp.partition] = EditWarDetector()
            offset = committed.get(tp.partition, -1)
            if offset is None or offset < 0:
                continue
            result = self.replayer.replay(
                tp.partition, offset, lambda msg, d=detector: self._replay_into(d, msg)
            )
            if result.replayed:
                log.info(
                    "warmed partition %d from %d events in %.1fs, %d pages with reverts",
                    tp.partition,
                    result.loaded,
                    result.seconds,
                    len(detector.pages),
                )

    @staticmethod
    def _replay_into(detector: EditWarDetector, msg) -> bool:
        try:
            event = CleanEvent.from_json(msg.value())
        except (ValueError, TypeError):
            return False
        _, message_time = msg.timestamp()
        detector.observe(event, event_time_ms(event, message_time))
        return True

    def partitions_revoked(self, partitions) -> None:
        for tp in partitions:
            self.detectors.pop(tp.partition, None)

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

    def periodic(self) -> None:
        now = time.monotonic()
        if now - self._last_expire >= EXPIRE_INTERVAL_SEC:
            expired = sum(d.expire() for d in self.detectors.values())
            if expired:
                log.debug("expired %d quiet pages", expired)
            self._last_expire = now

    def summary(self) -> str:
        tracked = sum(len(d.pages) for d in self.detectors.values())
        return f"{self.alerts} alerts raised, {tracked} pages tracked"


def main() -> None:
    instance = sys.argv[1] if len(sys.argv) > 1 else INSTANCE
    processor = EditWarProcessor(instance)
    signal.signal(signal.SIGINT, processor.stop)
    signal.signal(signal.SIGTERM, processor.stop)
    processor.run()


if __name__ == "__main__":
    main()
