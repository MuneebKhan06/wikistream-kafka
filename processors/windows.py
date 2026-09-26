"""Counts edits per page per minute, and tracks where it is safe to commit.

A window closes once the stream has moved past its minute by the lateness
allowance, measured against the largest event time seen rather than the wall
clock, so a replay closes windows at the same points as a live run.

Offsets are the interesting part. A window that is still open must be
recomputed in full after a restart, so the committed offset for a partition
is the lowest offset any open window depends on, not the last offset read.
Everything from that point is read again, which is why the counts are
written as totals for the minute instead of increments.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional, Tuple

MINUTE_MS = 60_000
DEFAULT_LATENESS_MS = 2 * MINUTE_MS
MAX_WINDOWS_PER_PARTITION = 200_000

WindowKey = Tuple[str, str, int]


@dataclass
class Window:
    count: int = 0
    first_offset: int = -1

    def add(self, offset: int) -> None:
        self.count += 1
        if self.first_offset < 0 or offset < self.first_offset:
            self.first_offset = offset


@dataclass
class ClosedWindow:
    wiki: str
    title: str
    minute_ms: int
    count: int

    @property
    def minute_iso(self) -> str:
        return datetime.fromtimestamp(self.minute_ms / 1000, tz=timezone.utc).isoformat()

    def as_row(self) -> tuple:
        return (self.wiki, self.title, self.minute_iso, self.count)


@dataclass
class PartitionState:
    windows: Dict[WindowKey, Window] = field(default_factory=dict)
    last_offset: int = -1
    flushed_minutes: set = field(default_factory=set)
    # Lowest offset behind a partial total written by a final flush. The
    # commit point must never pass it, or that total stays partial forever.
    partial_floor: int = -1


def minute_start(event_time_ms: int) -> int:
    return (event_time_ms // MINUTE_MS) * MINUTE_MS


class MinuteWindows:
    def __init__(
        self,
        lateness_ms: int = DEFAULT_LATENESS_MS,
        max_windows: int = MAX_WINDOWS_PER_PARTITION,
    ):
        self.lateness_ms = lateness_ms
        self.max_windows = max_windows
        self.partitions: Dict[int, PartitionState] = {}
        self.watermark_ms = 0
        self.late_events = 0
        self.counted = 0

    def state(self, partition: int) -> PartitionState:
        return self.partitions.setdefault(partition, PartitionState())

    def add(self, partition: int, offset: int, wiki: str, title: str, event_time_ms: int) -> bool:
        """Count one edit. Returns False when the event arrived too late."""
        state = self.state(partition)
        state.last_offset = max(state.last_offset, offset)
        self.watermark_ms = max(self.watermark_ms, event_time_ms)

        minute = minute_start(event_time_ms)
        key = (wiki, title, minute)
        already_written = minute in state.flushed_minutes and key not in state.windows
        if minute < self.closed_before() or already_written:
            # The minute is past the close cutoff, or was forced out by the
            # window cap, so counting this event could only produce a partial
            # total for a minute that is already written.
            self.late_events += 1
            return False

        state.windows.setdefault(key, Window()).add(offset)
        self.counted += 1
        self._enforce_cap(state)
        return True

    def _enforce_cap(self, state: PartitionState) -> None:
        """Force out the oldest minute if one partition holds too many pages."""
        while len(state.windows) > self.max_windows:
            oldest = min(key[2] for key in state.windows)
            for key in [k for k in state.windows if k[2] == oldest]:
                del state.windows[key]
            state.flushed_minutes.add(oldest)

    def closed_before(self) -> int:
        """Minutes starting before this are finished, given the lateness allowance."""
        return minute_start(self.watermark_ms - self.lateness_ms)

    def pop_closed(self, final: bool = False) -> list:
        """Remove and return every window the stream has moved past.

        With final=True every window is returned, open ones included. That is
        safe because a write keeps the larger total for a minute, so the
        partial count written here is replaced by the complete count when the
        next owner replays those events from the committed offset.
        """
        cutoff = self.closed_before()
        closed = []
        for state in self.partitions.values():
            done = [
                key for key in state.windows if final or key[2] < cutoff
            ]
            for key in done:
                wiki, title, minute = key
                window = state.windows.pop(key)
                state.flushed_minutes.add(minute)
                closed.append(ClosedWindow(wiki, title, minute, window.count))
                if final and minute >= cutoff:
                    state.partial_floor = (
                        window.first_offset
                        if state.partial_floor < 0
                        else min(state.partial_floor, window.first_offset)
                    )
        self._trim_flushed(cutoff)
        return closed

    def _trim_flushed(self, cutoff: int) -> None:
        """Minutes older than the cutoff are rejected anyway, so forget them."""
        for state in self.partitions.values():
            state.flushed_minutes = {m for m in state.flushed_minutes if m >= cutoff}

    def commit_offsets(self) -> Dict[int, int]:
        """Offset to commit per partition: the oldest offset still needed."""
        offsets = {}
        for partition, state in self.partitions.items():
            if state.windows:
                offset = min(w.first_offset for w in state.windows.values())
            elif state.last_offset >= 0:
                offset = state.last_offset + 1
            else:
                continue
            if state.partial_floor >= 0:
                offset = min(offset, state.partial_floor)
            offsets[partition] = offset
        return offsets

    def open_windows(self, partition: Optional[int] = None) -> int:
        if partition is not None:
            return len(self.state(partition).windows)
        return sum(len(s.windows) for s in self.partitions.values())

    def revoke(self, partitions: Iterable[int]) -> None:
        """Drop state for partitions now owned by another instance."""
        for partition in partitions:
            self.partitions.pop(partition, None)

    def stats(self) -> dict:
        return {
            "counted": self.counted,
            "open_windows": self.open_windows(),
            "late_events": self.late_events,
            "watermark_ms": self.watermark_ms,
        }
