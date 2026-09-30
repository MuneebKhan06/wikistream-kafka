"""Decides what the compacted wiki.page-latest topic should hold for a page.

The topic is keyed by page. Kafka compaction keeps at least the newest
record per key, so the topic converges on one record per page: the latest
edit. A record with a null value is a tombstone, which tells compaction to
remove the key entirely once the tombstone itself has aged out.

Only content changes update a page. Log events are mostly about users or
files, not the page text, and category membership changes are reported
against the category page, so neither is an edit to the page. The one log
event that matters is a deletion: the page no longer exists, so it gets a
tombstone instead of lingering in the snapshot forever. A restore brings
the page back with its next edit.
"""

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional, Tuple

from common.models import CleanEvent

CONTENT_TYPES = {"edit", "new"}
DELETE = ("delete", "delete")


@dataclass(frozen=True)
class PageLatest:
    wiki: str
    title: str
    event_id: str
    type: str
    user: str
    bot: bool
    minor: bool
    is_revert: bool
    comment: str
    event_time: str
    rev_id: Optional[int]
    length: Optional[int]

    @property
    def page_key(self) -> str:
        return f"{self.wiki}:{self.title}"

    @classmethod
    def from_event(cls, event: CleanEvent) -> "PageLatest":
        return cls(
            wiki=event.wiki,
            title=event.title,
            event_id=event.event_id,
            type=event.type,
            user=event.user,
            bot=event.bot,
            minor=event.minor,
            is_revert=event.is_revert,
            comment=event.comment,
            event_time=event.event_time,
            rev_id=event.rev_new,
            length=event.length_new,
        )

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), ensure_ascii=False).encode("utf-8")

    @classmethod
    def from_json(cls, payload: bytes) -> "PageLatest":
        return cls(**json.loads(payload))


def event_ms(event_time: str) -> int:
    """Epoch milliseconds for an ISO event time, for ordering updates."""
    return int(datetime.fromisoformat(event_time).timestamp() * 1000)


def is_deletion(event: CleanEvent) -> bool:
    return event.type == "log" and (event.log_type, event.log_action) == DELETE


def page_update(event: CleanEvent) -> Optional[Tuple[str, Optional[PageLatest]]]:
    """What to write for this event, if anything.

    Returns (key, record) for a content change, (key, None) for a deletion
    tombstone, and None when the event does not change the page.
    """
    if event.type in CONTENT_TYPES:
        return event.page_key, PageLatest.from_event(event)
    if is_deletion(event):
        return event.page_key, None
    return None
