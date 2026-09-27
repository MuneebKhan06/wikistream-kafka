"""Spots edit wars: repeated reverts on one page by more than one person.

The rule follows Wikipedia's own three revert rule. A page is flagged when
it collects `min_reverts` reverts inside `window_ms`, made by at least
`min_users` different people. One user reverting alone is a cleanup, not a
war. Bot reverts are skipped by default: an anti vandalism bot undoing
vandalism is enforcement.

Detection uses event time only, so replaying the same events always gives
the same alerts with the same ids. That is what makes restarts safe: state
rebuilt from history re-derives which pages were already alerted.
"""

import hashlib
import json
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional, Tuple

from common.models import CleanEvent

MINUTE_MS = 60_000
DEFAULT_WINDOW_MS = 30 * MINUTE_MS
DEFAULT_MIN_REVERTS = 3
DEFAULT_MIN_USERS = 2


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


@dataclass(frozen=True)
class Revert:
    event_id: str
    user: str
    time_ms: int


@dataclass(frozen=True)
class EditWarAlert:
    alert_id: str
    wiki: str
    title: str
    revert_count: int
    users: Tuple[str, ...]
    event_ids: Tuple[str, ...]
    window_start: str
    window_end: str

    @property
    def page_key(self) -> str:
        return f"{self.wiki}:{self.title}"

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), ensure_ascii=False).encode("utf-8")

    @classmethod
    def from_json(cls, payload: bytes) -> "EditWarAlert":
        data = json.loads(payload)
        data["users"] = tuple(data["users"])
        data["event_ids"] = tuple(data["event_ids"])
        return cls(**data)


def alert_id_for(wiki: str, title: str, first_event_id: str) -> str:
    """Same war, same id, however many times it is detected."""
    digest = hashlib.sha256(f"{wiki}\x00{title}\x00{first_event_id}".encode("utf-8"))
    return digest.hexdigest()[:32]


@dataclass
class PageState:
    reverts: Deque[Revert] = field(default_factory=deque)
    alerted_until_ms: int = -1


class EditWarDetector:
    def __init__(
        self,
        window_ms: int = DEFAULT_WINDOW_MS,
        min_reverts: int = DEFAULT_MIN_REVERTS,
        min_users: int = DEFAULT_MIN_USERS,
        ignore_bots: bool = True,
    ):
        self.window_ms = window_ms
        self.min_reverts = min_reverts
        self.min_users = min_users
        self.ignore_bots = ignore_bots
        self.pages: Dict[str, PageState] = {}
        self.watermark_ms = 0
        self.reverts_seen = 0
        self.alerts_raised = 0

    def observe(self, event: CleanEvent, time_ms: int) -> Optional[EditWarAlert]:
        """Feed one event; returns an alert when this revert starts a war."""
        self.watermark_ms = max(self.watermark_ms, time_ms)
        if not event.is_revert or (self.ignore_bots and event.bot):
            return None

        self.reverts_seen += 1
        state = self.pages.setdefault(event.page_key, PageState())
        state.reverts.append(Revert(event.event_id, event.user, time_ms))
        self._evict(state, time_ms)

        if time_ms <= state.alerted_until_ms:
            # Already alerted for this war; further reverts extend it quietly.
            state.alerted_until_ms = time_ms + self.window_ms
            return None

        users = {revert.user for revert in state.reverts}
        if len(state.reverts) < self.min_reverts or len(users) < self.min_users:
            return None

        reverts: List[Revert] = sorted(state.reverts, key=lambda r: (r.time_ms, r.event_id))
        state.alerted_until_ms = time_ms + self.window_ms
        self.alerts_raised += 1
        return EditWarAlert(
            alert_id=alert_id_for(event.wiki, event.title, reverts[0].event_id),
            wiki=event.wiki,
            title=event.title,
            revert_count=len(reverts),
            users=tuple(sorted(users)),
            event_ids=tuple(r.event_id for r in reverts),
            window_start=iso(reverts[0].time_ms),
            window_end=iso(reverts[-1].time_ms),
        )

    def _evict(self, state: PageState, now_ms: int) -> None:
        while state.reverts and state.reverts[0].time_ms < now_ms - self.window_ms:
            state.reverts.popleft()

    def expire(self) -> int:
        """Forget pages with no recent reverts, so memory tracks activity."""
        cutoff = self.watermark_ms - self.window_ms
        stale = [
            key
            for key, state in self.pages.items()
            if (not state.reverts or state.reverts[-1].time_ms < cutoff)
            and state.alerted_until_ms < self.watermark_ms
        ]
        for key in stale:
            del self.pages[key]
        return len(stale)

    def forget(self, page_keys) -> None:
        for key in page_keys:
            self.pages.pop(key, None)

    def stats(self) -> dict:
        return {
            "pages_tracked": len(self.pages),
            "reverts_seen": self.reverts_seen,
            "alerts_raised": self.alerts_raised,
        }
