"""Parsing and validation for Wikimedia recentchange events."""

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

EVENT_TYPES = {"edit", "new", "log", "categorize"}

REVERT_PATTERNS = re.compile(
    r"(^|\W)(revert(ed|ing)?|undid revision|undo|rv[tv]?|rollback)(\W|$)",
    re.IGNORECASE,
)


class ParseError(ValueError):
    """Raised when an event cannot be turned into a CleanEvent."""


@dataclass(frozen=True)
class CleanEvent:
    event_id: str
    wiki: str
    title: str
    type: str
    namespace: int
    user: str
    bot: bool
    minor: bool
    comment: str
    event_time: str
    rev_old: Optional[int]
    rev_new: Optional[int]
    length_old: Optional[int]
    length_new: Optional[int]
    server_name: str

    @property
    def page_key(self) -> str:
        return f"{self.wiki}:{self.title}"

    @property
    def is_revert(self) -> bool:
        return self.type == "edit" and bool(REVERT_PATTERNS.search(self.comment))

    @property
    def size_delta(self) -> Optional[int]:
        if self.length_old is None or self.length_new is None:
            return None
        return self.length_new - self.length_old

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), ensure_ascii=False).encode("utf-8")

    @classmethod
    def from_json(cls, payload: bytes) -> "CleanEvent":
        return cls(**json.loads(payload))


def event_id_of(raw: dict) -> str:
    try:
        event_id = raw["meta"]["id"]
    except (KeyError, TypeError) as exc:
        raise ParseError("missing meta.id") from exc
    if not isinstance(event_id, str) or not event_id:
        raise ParseError("meta.id is not a non-empty string")
    return event_id


def _optional_int(value) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ParseError(f"expected int, got {type(value).__name__}")
    return value


def _event_time(raw: dict) -> str:
    dt = raw.get("meta", {}).get("dt")
    if isinstance(dt, str):
        try:
            parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
            return parsed.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    ts = raw.get("timestamp")
    if isinstance(ts, int) and not isinstance(ts, bool):
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    raise ParseError("no usable event time")


def parse_raw(payload) -> dict:
    """Decode a raw Kafka value into a dict, raising ParseError on bad input."""
    if isinstance(payload, (bytes, bytearray)):
        try:
            payload = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ParseError("value is not valid utf-8") from exc
    try:
        raw = json.loads(payload)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ParseError(f"invalid json: {exc}") from exc
    if not isinstance(raw, dict):
        raise ParseError("event is not a json object")
    return raw


def clean_event(raw: dict) -> CleanEvent:
    """Validate a raw recentchange dict and normalise it into a CleanEvent."""
    event_id = event_id_of(raw)

    event_type = raw.get("type")
    if event_type not in EVENT_TYPES:
        raise ParseError(f"unknown event type: {event_type!r}")

    wiki = raw.get("wiki")
    title = raw.get("title")
    if not isinstance(wiki, str) or not wiki:
        raise ParseError("missing wiki")
    if not isinstance(title, str) or not title.strip():
        raise ParseError("missing title")

    namespace = raw.get("namespace")
    if isinstance(namespace, bool) or not isinstance(namespace, int):
        raise ParseError("namespace is not an int")

    revision = raw.get("revision") or {}
    length = raw.get("length") or {}

    return CleanEvent(
        event_id=event_id,
        wiki=wiki,
        title=" ".join(title.split()),
        type=event_type,
        namespace=namespace,
        user=str(raw.get("user") or ""),
        bot=bool(raw.get("bot", False)),
        minor=bool(raw.get("minor", False)),
        comment=str(raw.get("comment") or "").strip(),
        event_time=_event_time(raw),
        rev_old=_optional_int(revision.get("old")),
        rev_new=_optional_int(revision.get("new")),
        length_old=_optional_int(length.get("old")),
        length_new=_optional_int(length.get("new")),
        server_name=str(raw.get("server_name") or ""),
    )
