import json

from processors.dedup import DedupCache
from processors.warmup import CacheWarmer

BASE = 1_790_000_000_000


class FakeMessage:
    def __init__(self, offset, event_id, timestamp=BASE, value=None):
        self._offset = offset
        self._value = (
            value
            if value is not None
            else json.dumps({"meta": {"id": event_id}}).encode("utf-8")
        )
        self._timestamp = timestamp

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def timestamp(self):
        return (1, self._timestamp)

    def error(self):
        return None


class FakeConsumer:
    """Serves a fixed partition log, tracking what the warmer asked for."""

    def __init__(self, messages, low=0, time_offset=0):
        self.messages = messages
        self.low = low
        self.time_offset = time_offset
        self.assigned_from = None
        self.closed = False
        self._queue = []

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        high = self.messages[-1].offset() + 1 if self.messages else self.low
        return (self.low, high)

    def offsets_for_times(self, partitions, timeout=None):
        tp = partitions[0]
        tp.offset = self.time_offset
        return [tp]

    def assign(self, partitions):
        self.assigned_from = partitions[0].offset
        self._queue = [
            m for m in self.messages if m.offset() >= partitions[0].offset
        ]

    def poll(self, timeout=None):
        return self._queue.pop(0) if self._queue else None

    def close(self):
        self.closed = True


def warmer_for(consumer, **kwargs):
    return CacheWarmer(
        consumer_factory=lambda: consumer, now_ms=lambda: BASE, **kwargs
    )


def test_ids_before_the_commit_point_are_loaded():
    messages = [FakeMessage(i, f"id-{i}") for i in range(10)]
    consumer = FakeConsumer(messages)
    cache = DedupCache()

    result = warmer_for(consumer).warm(0, cache, until_offset=10)

    assert result.loaded == 10
    assert cache.is_new("id-3", BASE) is False
    assert consumer.closed is True


def test_records_past_the_commit_point_are_not_loaded():
    """Uncommitted records are not in wiki.clean, so they must stay new."""
    messages = [FakeMessage(i, f"id-{i}") for i in range(10)]
    cache = DedupCache()

    result = warmer_for(FakeConsumer(messages)).warm(0, cache, until_offset=6)

    assert result.loaded == 6
    assert cache.is_new("id-5", BASE) is False
    assert cache.is_new("id-6", BASE) is True
    assert cache.is_new("id-9", BASE) is True


def test_nothing_committed_means_no_replay():
    consumer = FakeConsumer([FakeMessage(0, "id-0")])
    result = warmer_for(consumer).warm(0, DedupCache(), until_offset=0)
    assert result.loaded == 0
    assert consumer.assigned_from is None


def test_replay_starts_at_the_window_boundary():
    messages = [FakeMessage(i, f"id-{i}") for i in range(100)]
    consumer = FakeConsumer(messages, time_offset=70)
    result = warmer_for(consumer).warm(0, DedupCache(), until_offset=100)
    assert consumer.assigned_from == 70
    assert result.from_offset == 70
    assert result.loaded == 30


def test_replay_is_capped_to_max_records():
    messages = [FakeMessage(i, f"id-{i}") for i in range(100)]
    consumer = FakeConsumer(messages, time_offset=0)
    result = warmer_for(consumer, max_records=25).warm(0, DedupCache(), until_offset=100)
    assert consumer.assigned_from == 75
    assert result.loaded == 25


def test_empty_partition_is_skipped():
    consumer = FakeConsumer([], low=0)
    result = warmer_for(consumer).warm(0, DedupCache(), until_offset=0)
    assert result.replayed == 0


def test_unparseable_records_are_counted_but_do_not_stop_the_replay():
    messages = [
        FakeMessage(0, "id-0"),
        FakeMessage(1, None, value=b"{broken"),
        FakeMessage(2, "id-2"),
    ]
    result = warmer_for(FakeConsumer(messages)).warm(0, DedupCache(), until_offset=3)
    assert result.loaded == 2
    assert result.skipped == 1
