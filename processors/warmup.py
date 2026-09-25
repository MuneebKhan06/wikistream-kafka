"""Rebuilds dedup state for a partition by replaying its recent history.

The dedup cache lives in memory, so a restarting cleaner starts blind and
cannot recognise source duplicates of events its predecessor already
handled. Before processing a newly assigned partition, the cleaner replays
the recent history of that same wiki.raw partition and loads the ids.

Replaying the input topic works because wiki.raw is keyed by event id, so
every copy of an event is on the same partition as its original. Nothing is
produced and no offsets are committed: this only fills memory.

The replay stops at the committed offset, never at the end of the
partition. Records past the commit point were either never processed or
belonged to an aborted transaction, so they are not in wiki.clean. Loading
their ids would make the cleaner drop them as duplicates and lose them.
"""

import time
from dataclasses import dataclass
from typing import Callable, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from common.config import TOPIC_RAW, consumer_config
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


def default_consumer_factory() -> Consumer:
    return Consumer(
        consumer_config(
            "cleaner-warmup",
            **{"group.id": "cleaner-warmup", "enable.partition.eof": True},
        )
    )


class CacheWarmer:
    def __init__(
        self,
        topic: str = TOPIC_RAW,
        window_ms: int = HOUR_MS,
        max_records: int = MAX_RECORDS,
        consumer_factory: Callable[[], Consumer] = default_consumer_factory,
        now_ms: Callable[[], int] = lambda: int(time.time() * 1000),
    ):
        self.topic = topic
        self.window_ms = window_ms
        self.max_records = max_records
        self.consumer_factory = consumer_factory
        self.now_ms = now_ms

    def start_offset(
        self, consumer: Consumer, partition: int, until_offset: int
    ) -> Optional[int]:
        """First offset inside the window, or None when there is nothing to load."""
        low, _ = consumer.get_watermark_offsets(
            TopicPartition(self.topic, partition), timeout=10, cached=False
        )
        if until_offset <= low:
            return None

        cutoff = self.now_ms() - self.window_ms
        found = consumer.offsets_for_times(
            [TopicPartition(self.topic, partition, cutoff)], timeout=10
        )[0]
        start = low if found.offset < 0 else max(found.offset, low)
        start = max(start, until_offset - self.max_records)
        return start if start < until_offset else None

    def warm(self, partition: int, cache: DedupCache, until_offset: int) -> WarmupResult:
        """Load ids from [window start, until_offset) into the cache."""
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
                self._load(msg, cache, result)
        finally:
            consumer.close()
            result.seconds = time.monotonic() - started
        return result

    def _load(self, msg, cache: DedupCache, result: WarmupResult) -> None:
        _, message_time = msg.timestamp()
        try:
            event_id = event_id_of(parse_raw(msg.value()))
        except ParseError:
            result.skipped += 1
            return
        cache.is_new(event_id, message_time if message_time > 0 else self.now_ms())
        result.loaded += 1
