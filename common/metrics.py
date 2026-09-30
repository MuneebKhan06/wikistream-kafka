"""Throughput and consumer lag logging shared by the pipeline processes."""

import logging
import time
from typing import Iterable, Optional

from confluent_kafka import Consumer, KafkaException, TopicPartition

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def setup_logging(name: str, level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(level=level, format=LOG_FORMAT)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    return logging.getLogger(name)


class RateMeter:
    """Counts events and logs a rate line every `interval` seconds."""

    def __init__(self, log: logging.Logger, label: str, interval: float = 10.0):
        self.log = log
        self.label = label
        self.interval = interval
        self.total = 0
        self.errors = 0
        self._window_count = 0
        self._started = time.monotonic()
        self._last_report = self._started

    def mark(self, count: int = 1) -> None:
        self.total += count
        self._window_count += count
        self.report()

    def mark_error(self, count: int = 1) -> None:
        self.errors += count

    def report(self, force: bool = False) -> None:
        now = time.monotonic()
        elapsed = now - self._last_report
        if not force and elapsed < self.interval:
            return
        if elapsed <= 0:
            return
        rate = self._window_count / elapsed
        overall = self.total / max(now - self._started, 1e-9)
        self.log.info(
            "%s: %.1f events/sec (window), %.1f avg, %d total, %d errors",
            self.label,
            rate,
            overall,
            self.total,
            self.errors,
        )
        self._window_count = 0
        self._last_report = now


def partition_lag(consumer: Consumer, partitions: Iterable[TopicPartition]) -> dict:
    """Return {(topic, partition): lag}, leaving out partitions it could not read.

    Lag is reporting, not processing, so it must never take the process down.
    During a leader change the broker a request reaches may no longer lead the
    partition and answers NOT_LEADER_FOR_PARTITION. That partition is skipped
    this time; the next report reaches the new leader.
    """
    lag = {}
    for tp in partitions:
        try:
            position = consumer.position([tp])[0].offset
            _, high = consumer.get_watermark_offsets(tp, timeout=5, cached=False)
            if position is None or position < 0:
                committed = consumer.committed([tp], timeout=5)[0].offset
                position = committed if committed and committed >= 0 else high
        except KafkaException:
            continue
        lag[(tp.topic, tp.partition)] = max(high - position, 0)
    return lag


def log_lag(
    log: logging.Logger,
    consumer: Consumer,
    partitions: Optional[Iterable[TopicPartition]] = None,
) -> int:
    """Log lag per partition and return the total of those it could read."""
    partitions = list(partitions if partitions is not None else consumer.assignment())
    if not partitions:
        return 0
    lag = partition_lag(consumer, partitions)
    detail = ", ".join(f"p{p}={value}" for (_, p), value in sorted(lag.items()))
    missing = sorted({tp.partition for tp in partitions} - {p for _, p in lag})
    if missing:
        detail += f"; unavailable for {', '.join(f'p{p}' for p in missing)}"
    total = sum(lag.values())
    log.info("lag total=%d (%s)", total, detail)
    return total
