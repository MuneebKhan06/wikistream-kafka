"""The questions the dashboard asks PostgreSQL, and the shapes it answers in.

Time windows are measured on event_time, the moment the edit happened on
the wiki, which is indexed. The live pipeline stores an edit within a couple
of seconds, so the last N minutes of event time is the last N minutes of
traffic. Windows are clamped, so a request cannot ask for a full table scan.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

MIN_MINUTES = 5
# Six hours of live traffic is about 800,000 rows, a few seconds to count.
# A full day would run into the statement timeout.
MAX_MINUTES = 6 * 60


def clamp_minutes(minutes: int) -> int:
    return max(MIN_MINUTES, min(MAX_MINUTES, int(minutes)))


def minute_series(rows: list, minutes: int, now: datetime) -> list:
    """Per minute counts for the window, with empty minutes filled in as zero.

    The last, still filling minute is left out: a partial count would always
    read as a sudden drop at the right edge.
    """
    end = now.replace(second=0, microsecond=0)
    start = end - timedelta(minutes=minutes)
    counts = {row["minute"].astimezone(timezone.utc): row["edits"] for row in rows}
    series = []
    minute = start
    while minute < end:
        series.append({"minute": minute.isoformat(), "edits": int(counts.get(minute, 0))})
        minute += timedelta(minutes=1)
    return series


# The window is read once and counted several ways. Distinct pages are
# counted through a hashed DISTINCT rather than COUNT(DISTINCT ...), which
# sorts: over an hour of live traffic that is about 0.3 s instead of 1.1 s.
OVERVIEW_TOTALS = """
    WITH window_edits AS MATERIALIZED (
        SELECT wiki, title, bot FROM edits
        WHERE event_time > %(since)s AND event_time <= %(now)s
    )
    SELECT
        (SELECT COUNT(*) FROM window_edits) AS edits,
        (SELECT COUNT(*) FROM window_edits WHERE bot) AS bot_edits,
        (SELECT COUNT(*) FROM (SELECT DISTINCT wiki FROM window_edits) AS w) AS wikis,
        (SELECT COUNT(*) FROM (SELECT DISTINCT wiki, title FROM window_edits) AS p) AS pages
"""

OVERVIEW_PER_MINUTE = """
    SELECT date_trunc('minute', event_time) AS minute, COUNT(*) AS edits
    FROM edits
    WHERE event_time > %(since)s AND event_time <= %(now)s
    GROUP BY 1
"""

LAST_MINUTE = """
    SELECT COUNT(*) AS edits FROM edits
    WHERE event_time > %(now)s - INTERVAL '60 seconds' AND event_time <= %(now)s
"""

STORED = """
    SELECT
        (SELECT COUNT(*) FROM edits) AS edits_stored,
        (SELECT MAX(ingested_at) FROM edits) AS newest_stored_at,
        (SELECT MAX(event_time) FROM edits) AS newest_event_time,
        (SELECT COUNT(*) FROM alerts
         WHERE window_end > %(since)s AND window_start <= %(now)s) AS edit_wars
"""


def iso(value: Optional[datetime]) -> Optional[str]:
    return value.astimezone(timezone.utc).isoformat() if value else None


def overview(db, minutes: int, now: Optional[datetime] = None) -> dict:
    now = now or datetime.now(timezone.utc)
    minutes = clamp_minutes(minutes)
    since = now - timedelta(minutes=minutes)
    params = {"since": since, "now": now}

    totals = db.one(OVERVIEW_TOTALS, params)
    stored = db.one(STORED, params)
    last_minute = db.one(LAST_MINUTE, params)
    series = minute_series(db.rows(OVERVIEW_PER_MINUTE, params), minutes, now)

    edits = int(totals.get("edits") or 0)
    bots = int(totals.get("bot_edits") or 0)
    return {
        "minutes": minutes,
        "now": iso(now),
        "edits": edits,
        "bot_share": round(bots / edits, 4) if edits else 0.0,
        "wikis": int(totals.get("wikis") or 0),
        "pages": int(totals.get("pages") or 0),
        "edits_per_second": round(int(last_minute.get("edits") or 0) / 60, 2),
        "edit_wars": int(stored.get("edit_wars") or 0),
        "edits_stored": int(stored.get("edits_stored") or 0),
        "newest_stored_at": iso(stored.get("newest_stored_at")),
        "newest_event_time": iso(stored.get("newest_event_time")),
        "per_minute": series,
    }
