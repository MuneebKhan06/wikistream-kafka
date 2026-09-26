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

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition  # noqa: E402

from common.config import TOPIC_CLEAN, consumer_config  # noqa: E402
from common.metrics import RateMeter, log_lag, setup_logging  # noqa: E402
from common.models import CleanEvent  # noqa: E402
from processors.transform import event_time_ms  # noqa: E402
from processors.windows import MinuteWindows  # noqa: E402
from sinks.db import upsert_trending, wait_for_database  # noqa: E402

log = setup_logging("trending")

GROUP_ID = "trending"
POLL_TIMEOUT_SEC = 1.0
FLUSH_INTERVAL_SEC = 5.0
LAG_INTERVAL_SEC = 30.0


class Trending:
    def __init__(self):
        self.consumer = Consumer(consumer_config(GROUP_ID))
        self.connection = wait_for_database(log=log)
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
            upsert_trending(self.connection, rows)
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
        self.consumer.subscribe(
            [TOPIC_CLEAN], on_assign=self.on_assign, on_revoke=self.on_revoke
        )
        log.info("trending started, group %s", GROUP_ID)

        while self.running:
            msg = self.consumer.poll(POLL_TIMEOUT_SEC)
            if msg is None:
                self._periodic()
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            self.handle(msg)
            self._periodic()

        self.shutdown()

    def shutdown(self) -> None:
        self.flush(final=True)
        self.consumer.close()
        self.connection.close()
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
