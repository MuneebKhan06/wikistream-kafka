"""Bounded, time windowed set of event ids seen on one partition.

Duplicates of an event always land on the same partition as the original,
because wiki.raw is keyed by event id, so the cleaner never needs a shared
store to spot them. Ids are held in time buckets: dropping a whole bucket
expires a slice of the window in one step, with no per id bookkeeping.
"""

from collections import OrderedDict
from dataclasses import dataclass, field

HOUR_MS = 60 * 60 * 1000


@dataclass
class DedupCache:
    window_ms: int = HOUR_MS
    buckets: int = 12
    max_ids: int = 500_000
    _buckets: "OrderedDict[int, set]" = field(default_factory=OrderedDict)
    seen_count: int = 0
    duplicate_count: int = 0
    evicted_early: int = 0

    @property
    def bucket_span_ms(self) -> int:
        return max(self.window_ms // self.buckets, 1)

    @property
    def size(self) -> int:
        return sum(len(ids) for ids in self._buckets.values())

    def is_new(self, event_id: str, event_time_ms: int) -> bool:
        """Record an id, returning False when it was already seen."""
        self.seen_count += 1
        self._expire(event_time_ms)

        for ids in self._buckets.values():
            if event_id in ids:
                self.duplicate_count += 1
                return False

        index = event_time_ms // self.bucket_span_ms
        self._buckets.setdefault(index, set()).add(event_id)
        self._enforce_size()
        return True

    def _expire(self, now_ms: int) -> None:
        cutoff = (now_ms - self.window_ms) // self.bucket_span_ms
        for index in [i for i in self._buckets if i < cutoff]:
            del self._buckets[index]

    def _enforce_size(self) -> None:
        """Guard against memory growth if the stream bursts or clocks jump."""
        while self.size > self.max_ids and len(self._buckets) > 1:
            _, dropped = self._buckets.popitem(last=False)
            self.evicted_early += len(dropped)

    def clear(self) -> None:
        """Called when a partition is revoked: the state belongs to its owner."""
        self._buckets.clear()

    def stats(self) -> dict:
        return {
            "ids": self.size,
            "seen": self.seen_count,
            "duplicates": self.duplicate_count,
            "evicted_early": self.evicted_early,
        }
