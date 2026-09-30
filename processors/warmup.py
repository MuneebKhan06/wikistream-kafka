"""Rebuilds in memory state for a partition by replaying its recent history.

Stateful consumers keep their state in memory, so a restarting instance
starts blind. Before processing a newly assigned partition, it replays the
recent history of that partition and feeds it back into its state. Nothing
is produced and no offsets are committed: this only fills memory.

The replay stops at the committed offset, never at the end of the
partition. Records past the commit point were either never processed or
belonged to an aborted transaction. Loading them would make the consumer
believe it had already handled them. For the cleaner that means dropping
them as duplicates and losing them.

The window is measured back from the time of the last committed record,
not from the wall clock. A consumer catching up on hours of backlog needs
the history just behind its own position, which a wall clock cutoff would
skip entirely.
"""

import time
from dataclasses import dataclass
from typing import Callable, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from common.config import TOPIC_RAW, consumer_config, scoped
from common.models import ParseError, event_id_of, parse_raw
from processors.dedup import HOUR_MS, DedupCache

POLL_TIMEOUT_SEC = 5.0
MAX_RECORDS = 200_000


@dataclass
class WarmupResult:
    partition: int
    loaded: int = 0
    skipped: int = 0
    from_offset: int = -1
    until_offset: int = -1
    seconds: float = 0.0

    @property
    def replayed(self) -> int:
        return self.loaded + self.skipped


def consumer_factory_for(group_id: str) -> Callable[[], Consumer]:
    def factory() -> Consumer:
        return Consumer(
            consumer_config(group_id, **{"group.id": group_id, "enable.partition.eof": True})
        )

    return factory


default_consumer_factory = consumer_factory_for(scoped("cleaner-warmup"))


class PartitionReplayer:
    """Reads [window start, until_offset) of one partition into a callback."""

    def __init__(
        self,
        topic: str,
        window_ms: int,
        max_records: int = MAX_RECORDS,
        consumer_factory: Callable[[], Consumer] = default_consumer_factory,
        now_ms: Callable[[], int] = lambda: int(time.time() * 1000),
    ):
        self.topic = topic
        self.window_ms = window_ms
        self.max_records = max_records
        self.consumer_factory = consumer_factory
        self.now_ms = now_ms

    def anchor_time(self, consumer: Consumer, partition: int, until_offset: int) -> int:
        """Timestamp of the last committed record, where the window ends."""
        consumer.assign([TopicPartition(self.topic, partition, until_offset - 1)])
        msg = consumer.poll(POLL_TIMEOUT_SEC)
        if msg is not None and not msg.error():
            _, timestamp = msg.timestamp()
            if timestamp > 0:
                return timestamp
        return self.now_ms()

    def start_offset(
        self, consumer: Consumer, partition: int, until_offset: int
    ) -> Optional[int]:
        """First offset inside the window, or None when there is nothing to load."""
        low, _ = consumer.get_watermark_offsets(
            TopicPartition(self.topic, partition), timeout=10, cached=False
        )
        if until_offset <= low:
            return None

        cutoff = self.anchor_time(consumer, partition, until_offset) - self.window_ms
        found = consumer.offsets_for_times(
            [TopicPartition(self.topic, partition, cutoff)], timeout=10
        )[0]
        if found.offset < 0:
            # No record is newer than the cutoff, so nothing is in the window.
            return None
        start = max(found.offset, low, until_offset - self.max_records)
        return start if start < until_offset else None

    def replay(
        self, partition: int, until_offset: int, on_message: Callable[[object], bool]
    ) -> WarmupResult:
        """on_message returns True when a record was used, False when skipped."""
        started = time.monotonic()
        result = WarmupResult(partition=partition, until_offset=until_offset)
        if until_offset <= 0:
            return result

        consumer = self.consumer_factory()
        try:
            start = self.start_offset(consumer, partition, until_offset)
            if start is None:
                return result

            result.from_offset = start
            consumer.assign([TopicPartition(self.topic, partition, start)])

            while True:
                msg = consumer.poll(POLL_TIMEOUT_SEC)
                if msg is None:
                    break
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    raise KafkaException(msg.error())
                if msg.offset() >= until_offset:
                    break
                if on_message(msg):
                    result.loaded += 1
                else:
                    result.skipped += 1
        finally:
            consumer.close()
            result.seconds = time.monotonic() - started
        return result


class CacheWarmer(PartitionReplayer):
    """Refills the cleaner's dedup cache from wiki.raw.

    wiki.raw is keyed by event id, so every copy of an event is on the same
    partition as its original, and one partition's history is enough.
    """

    def __init__(
        self,
        topic: str = TOPIC_RAW,
        window_ms: int = HOUR_MS,
        max_records: int = MAX_RECORDS,
        consumer_factory: Callable[[], Consumer] = default_consumer_factory,
        now_ms: Callable[[], int] = lambda: int(time.time() * 1000),
    ):
        super().__init__(topic, window_ms, max_records, consumer_factory, now_ms)

    def warm(self, partition: int, cache: DedupCache, until_offset: int) -> WarmupResult:
        """Load ids from [window start, until_offset) into the cache."""
        return self.replay(partition, until_offset, lambda msg: self._load(msg, cache))

    def _load(self, msg, cache: DedupCache) -> bool:
        _, message_time = msg.timestamp()
        try:
            event_id = event_id_of(parse_raw(msg.value()))
        except ParseError:
            return False
        cache.is_new(event_id, message_time if message_time > 0 else self.now_ms())
        return True
