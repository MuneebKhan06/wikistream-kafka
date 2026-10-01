"""Turns one raw Kafka message into an outcome the cleaner can act on.

Kept free of Kafka clients so the decision logic can be tested directly:
every raw message becomes exactly one Clean, Duplicate, Rejected or Skipped
result.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union

from common.dlq import dlq_record
from common.models import CleanEvent, ParseError, clean_event, is_canary, parse_raw
from processors.dedup import DedupCache


@dataclass(frozen=True)
class Clean:
    event: CleanEvent

    @property
    def key(self) -> str:
        return self.event.page_key


@dataclass(frozen=True)
class Duplicate:
    event_id: str


@dataclass(frozen=True)
class Rejected:
    reason: str
    payload: bytes

    def envelope(self, source_topic: str, partition: int, offset: int) -> bytes:
        """Wraps the bad payload with enough context to investigate later."""
        return dlq_record(
            "cleaner",
            self.reason,
            self.payload,
            source_topic,
            partition=partition,
            offset=offset,
        )


@dataclass(frozen=True)
class Skipped:
    """Valid input that is not an event, such as a stream heartbeat."""

    reason: str


Outcome = Union[Clean, Duplicate, Rejected, Skipped]


def event_time_ms(event: CleanEvent, fallback_ms: Optional[int] = None) -> int:
    try:
        return int(datetime.fromisoformat(event.event_time).timestamp() * 1000)
    except (ValueError, TypeError):
        return fallback_ms if fallback_ms is not None else 0


def transform(
    payload: bytes,
    cache: DedupCache,
    message_time_ms: Optional[int] = None,
) -> Outcome:
    """Parse, validate and deduplicate one raw message."""
    try:
        raw = parse_raw(payload)
        if is_canary(raw):
            # Not a failure: sending these to the DLQ would bury real ones.
            return Skipped("canary")
        event = clean_event(raw)
    except ParseError as exc:
        return Rejected(str(exc), payload if isinstance(payload, bytes) else b"")

    timestamp = event_time_ms(event, message_time_ms)
    if not cache.is_new(event.event_id, timestamp):
        return Duplicate(event.event_id)
    return Clean(event)
