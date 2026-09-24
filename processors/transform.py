"""Turns one raw Kafka message into an outcome the cleaner can act on.

Kept free of Kafka clients so the decision logic can be tested directly:
every raw message becomes exactly one Clean, Duplicate or Rejected result.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union

from common.models import CleanEvent, ParseError, clean_event, parse_raw
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
        try:
            original = self.payload.decode("utf-8")
        except UnicodeDecodeError:
            original = repr(self.payload)
        return json.dumps(
            {
                "reason": self.reason,
                "source_topic": source_topic,
                "partition": partition,
                "offset": offset,
                "payload": original,
            },
            ensure_ascii=False,
        ).encode("utf-8")


Outcome = Union[Clean, Duplicate, Rejected]


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
        event = clean_event(raw)
    except ParseError as exc:
        return Rejected(str(exc), payload if isinstance(payload, bytes) else b"")

    timestamp = event_time_ms(event, message_time_ms)
    if not cache.is_new(event.event_id, timestamp):
        return Duplicate(event.event_id)
    return Clean(event)
