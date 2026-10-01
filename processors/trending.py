"""Counts edits per page per minute from wiki.clean into PostgreSQL.

Keyed by page, wiki.clean puts every edit to one page in one partition, so
one consumer sees all of a page's edits and can count them from local state
with no coordination.

The offsets this commits come from the window state, not from the last
message read: a partition is only advanced past the oldest offset its open
windows still need. A restart therefore re-reads those events and rebuilds
each open window in full before writing it again.
"""

import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Consumer, TopicPartition  # noqa: E402

from common.config import TOPIC_CLEAN, consumer_config, scoped  # noqa: E402
from common.metrics import RateMeter, log_lag, setup_logging  # noqa: E402
from common.models import CleanEvent  # noqa: E402
from common.offsets import report_retention_gaps  # noqa: E402
from common.topics import (  # noqa: E402
    AssignmentWatchdog,
    check_consumer_error,
    wait_for_topics,
)
from processors.transform import event_time_ms  # noqa: E402
from processors.windows import MinuteWindows  # noqa: E402
from sinks.db import Database, upsert_trending  # noqa: E402

log = setup_logging("trending")

GROUP_ID = scoped("trending")
POLL_TIMEOUT_SEC = 1.0
FLUSH_INTERVAL_SEC = 5.0
LAG_INTERVAL_SEC = 30.0


class Trending:
    def __init__(self):
        self.consumer = Consumer(consumer_config(GROUP_ID))
        self.db = Database(log)
        self.windows = MinuteWindows()
        self.meter = RateMeter(log, "trending")
        self.running = True
        self.written_rows = 0
        self._last_flush = time.monotonic()
        self._last_lag_report = time.monotonic()

    def stop(self, *_):
        log.info("stop requested, flushing closed windows")
        self.running = False

    def on_assign(self, consumer, partitions):
        consumer.incremental_assign(partitions)
        log.info("assigned partitions: %s", sorted(p.partition for p in partitions))
        report_retention_gaps(log, consumer, partitions)

    def on_revoke(self, consumer, partitions):
        """Write what we have, then forget: the new owner replays our commit."""
        self.flush(final=True)
        self.windows.revoke([p.partition for p in partitions])
        consumer.incremental_unassign(partitions)
        log.info("revoked partitions: %s", sorted(p.partition for p in partitions))

    def handle(self, msg) -> None:
        try:
            event = CleanEvent.from_json(msg.value())
        except (ValueError, TypeError) as exc:
            log.error("unreadable record at offset %d: %s", msg.offset(), exc)
            self.meter.mark_error()
            return

        _, message_time = msg.timestamp()
        counted = self.windows.add(
            msg.partition(),
            msg.offset(),
            event.wiki,
            event.title,
            event_time_ms(event, message_time),
        )
        if counted:
            self.meter.mark()

    def flush(self, final: bool = False) -> int:
        """Write windows, then commit the offsets they no longer need.

        A final flush also writes windows that are still open, so a stop or a
        rebalance leaves the counts in PostgreSQL rather than waiting for the
        next owner to recompute them. The committed offsets do not move past
        those windows either way, so a partial total is always corrected.
        """
        closed = self.windows.pop_closed(final=final)
        if closed:
            rows = [window.as_row() for window in closed]
            self.db.write(upsert_trending, rows)
            self.written_rows += len(rows)
            log.info(
                "wrote %d page minutes, %d windows still open",
                len(rows),
                self.windows.open_windows(),
            )

        offsets = [
            TopicPartition(TOPIC_CLEAN, partition, offset)
            for partition, offset in self.windows.commit_offsets().items()
        ]
        if offsets:
            self.consumer.commit(offsets=offsets, asynchronous=False)
        self._last_flush = time.monotonic()
        return len(closed)

    def _periodic(self) -> None:
        now = time.monotonic()
        if now - self._last_flush >= FLUSH_INTERVAL_SEC:
            self.flush()
        if now - self._last_lag_report >= LAG_INTERVAL_SEC:
            log_lag(log, self.consumer, self.consumer.assignment())
            self._last_lag_report = now

    def run(self) -> None:
        wait_for_topics(self.consumer, [TOPIC_CLEAN], log)
        self._subscribe()
        watchdog = AssignmentWatchdog(self.consumer, GROUP_ID, [TOPIC_CLEAN], log, self._rejoin)
        log.info("trending started, group %s", GROUP_ID)

        while self.running:
            msg = self.consumer.poll(POLL_TIMEOUT_SEC)
            watchdog.check()
            if msg is None:
                self._periodic()
                continue
            if msg.error():
                check_consumer_error(msg, log)
                continue
            self.handle(msg)
            self._periodic()

        self.shutdown()

    def _subscribe(self) -> None:
        self.consumer.subscribe(
            [TOPIC_CLEAN], on_assign=self.on_assign, on_revoke=self.on_revoke
        )

    def _rejoin(self) -> None:
        self.consumer.unsubscribe()
        self._subscribe()

    def shutdown(self) -> None:
        self.flush(final=True)
        self.consumer.close()
        self.db.close()
        self.meter.report(force=True)
        stats = self.windows.stats()
        log.info(
            "stopped, %d page minutes written, %d events counted, %d dropped as late",
            self.written_rows,
            stats["counted"],
            stats["late_events"],
        )


def main() -> None:
    trending = Trending()
    signal.signal(signal.SIGINT, trending.stop)
    signal.signal(signal.SIGTERM, trending.stop)
    trending.run()


if __name__ == "__main__":
    main()
