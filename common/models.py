"""Parsing and validation for Wikimedia recentchange events."""

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

EVENT_TYPES = {"edit", "new", "log", "categorize"}

# MediaWiki writes revert summaries in the wiki's own language, so matching
# English alone misses most reverts on a global stream. These are the
# automatic summaries seen in real traffic. Phrases are kept specific so
# pages *about* reverts do not match: a Russian report titled "report on
# automatic reverts" or a talk page notice saying "your edit was reverted"
# is not itself a revert.
REVERT_PHRASES = {
    "en": r"(?:^|\W)(?:revert(?:ed|ing)?|undid revision|undo|rv[tv]?|rollback)(?:\W|$)",
    "wikidata": r"/\* (?:undo|restore):",
    "de": r"rückgängig gemacht|\bwurde verworfen\b",
    "es_pt": r"\brevertid[oa]s?\b|\bdeshecha la edici[oó]n|\bdesfeita a edi[cç][aã]o",
    "fr": r"\br[ée]vocation des modifications|\bannulation de la (?:\[\[[^\]|]*\|)?modification",
    "it": r"\bannullat[ae] l[ae] modific",
    "nl": r"\bongedaan gemaakt\b",
    "pl": r"\bwycofano (?:edycj|ostatni)|\banulowanie wersji",
    "cs": r"vr[aá]cen[ya]? do předchozího stavu",
    "ru": r"\bотмена правки\b|\bоткат правок\b",
    "zh": r"回退|撤销|撤銷|還原|还原",
    "ja": r"取り消し|巻き戻し",
}

REVERT_PATTERNS = re.compile(
    "|".join(f"(?:{pattern})" for pattern in REVERT_PHRASES.values()),
    re.IGNORECASE,
)


def is_revert_comment(comment: str) -> bool:
    return bool(REVERT_PATTERNS.search(comment or ""))


class ParseError(ValueError):
    """Raised when an event cannot be turned into a CleanEvent."""


CANARY_DOMAIN = "canary"


def is_canary(raw: dict) -> bool:
    """Wikimedia injects artificial heartbeat events to prove the stream is
    alive end to end. They are marked with meta.domain "canary", describe no
    real change, and consumers are expected to discard them."""
    meta = raw.get("meta")
    return isinstance(meta, dict) and meta.get("domain") == CANARY_DOMAIN


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
    # Only set on log events. Defaulted so records written before these
    # fields existed still decode.
    log_type: Optional[str] = None
    log_action: Optional[str] = None

    @property
    def page_key(self) -> str:
        return f"{self.wiki}:{self.title}"

    @property
    def is_revert(self) -> bool:
        return self.type == "edit" and is_revert_comment(self.comment)

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
        log_type=_optional_str(raw.get("log_type")),
        log_action=_optional_str(raw.get("log_action")),
    )


def _optional_str(value) -> Optional[str]:
    return value if isinstance(value, str) and value else None
