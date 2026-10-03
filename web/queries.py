"""The questions the dashboard asks PostgreSQL, and the shapes it answers in.

Time windows are measured on event_time, the moment the edit happened on
the wiki, which is indexed. The live pipeline stores an edit within a couple
of seconds, so the last N minutes of event time is the last N minutes of
traffic. Windows are clamped, so a request cannot ask for a full table scan.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote

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


# Trending pages, from the per minute totals the trending processor writes.
# A minute only appears once its window has closed, a couple of minutes
# behind the live edge, so newest_minute is returned for the page to show.

TOP_PAGES = """
    SELECT wiki, title, SUM(edit_count) AS edits
    FROM trending_minutes
    WHERE minute > %(since)s AND minute <= %(now)s
      AND (%(wiki)s IS NULL OR wiki = %(wiki)s)
    GROUP BY wiki, title
    ORDER BY edits DESC, wiki, title
    LIMIT %(limit)s
"""

PAGE_MINUTES = """
    SELECT wiki, title, minute, edit_count
    FROM trending_minutes
    WHERE minute > %(since)s AND minute <= %(now)s
      AND (wiki, title) IN (SELECT * FROM unnest(%(wikis)s::text[], %(titles)s::text[]))
"""

NEWEST_TRENDING_MINUTE = "SELECT MAX(minute) AS newest FROM trending_minutes"


def trending(db, minutes: int, wiki: Optional[str], limit: int, now: Optional[datetime] = None):
    now = now or datetime.now(timezone.utc)
    minutes = clamp_minutes(minutes)
    params = {
        "since": now - timedelta(minutes=minutes),
        "now": now,
        "wiki": wiki or None,
        "limit": max(1, min(int(limit), 50)),
    }
    top = db.rows(TOP_PAGES, params)
    series = {}
    if top:
        params["wikis"] = [row["wiki"] for row in top]
        params["titles"] = [row["title"] for row in top]
        by_page = {}
        for row in db.rows(PAGE_MINUTES, params):
            by_page.setdefault((row["wiki"], row["title"]), []).append(
                {"minute": row["minute"], "edits": row["edit_count"]}
            )
        series = {key: minute_series(rows, minutes, now) for key, rows in by_page.items()}

    newest = db.one(NEWEST_TRENDING_MINUTE).get("newest")
    return {
        "minutes": minutes,
        "wiki": wiki or None,
        "newest_minute": iso(newest),
        "pages": [
            {
                "wiki": row["wiki"],
                "title": row["title"],
                "edits": int(row["edits"]),
                "per_minute": [p["edits"] for p in series.get((row["wiki"], row["title"]), [])],
            }
            for row in top
        ],
    }


TOP_WIKIS = """
    SELECT wiki, COUNT(*) AS edits, COUNT(*) FILTER (WHERE bot) AS bot_edits
    FROM edits
    WHERE event_time > %(since)s AND event_time <= %(now)s
    GROUP BY wiki
    ORDER BY edits DESC, wiki
    LIMIT %(limit)s
"""


def wikis(db, minutes: int, limit: int = 20, now: Optional[datetime] = None) -> dict:
    now = now or datetime.now(timezone.utc)
    minutes = clamp_minutes(minutes)
    rows = db.rows(
        TOP_WIKIS,
        {"since": now - timedelta(minutes=minutes), "now": now, "limit": max(1, min(limit, 100))},
    )
    return {
        "minutes": minutes,
        "wikis": [
            {"wiki": r["wiki"], "edits": int(r["edits"]), "bot_edits": int(r["bot_edits"])}
            for r in rows
        ],
    }


# The live feed: newest edits first, with optional filters. Each filter
# narrows an index scan that walks backwards from now, so the cost depends
# on how far back the page of results reaches, not on the table size.

LATEST_EDITS = """
    SELECT event_id, wiki, title, type, namespace, username, bot, minor, is_revert,
           comment, event_time, length_old, length_new, server_name, ingested_at
    FROM edits
    WHERE (%(wiki)s IS NULL OR wiki = %(wiki)s)
      AND (NOT %(humans)s OR NOT bot)
      AND (NOT %(reverts)s OR is_revert)
      AND (%(before)s IS NULL OR event_time < %(before)s)
    ORDER BY event_time DESC
    LIMIT %(limit)s
"""


def page_url(server_name: str, title: str) -> Optional[str]:
    """MediaWiki's own form: spaces become underscores, the rest is percent encoded."""
    if not server_name:
        return None
    return f"https://{server_name}/wiki/{quote(title.replace(' ', '_'), safe=':/()_,!~*')}"


def latest_edits(
    db,
    limit: int = 50,
    wiki: Optional[str] = None,
    humans_only: bool = False,
    reverts_only: bool = False,
    before: Optional[datetime] = None,
) -> dict:
    rows = db.rows(
        LATEST_EDITS,
        {
            "wiki": wiki or None,
            "humans": humans_only,
            "reverts": reverts_only,
            "before": before,
            "limit": max(1, min(int(limit), 200)),
        },
    )
    edits = []
    for row in rows:
        size = None
        if row["length_old"] is not None and row["length_new"] is not None:
            size = row["length_new"] - row["length_old"]
        edits.append(
            {
                "event_id": row["event_id"],
                "wiki": row["wiki"],
                "title": row["title"],
                "type": row["type"],
                "user": row["username"],
                "bot": row["bot"],
                "minor": row["minor"],
                "is_revert": row["is_revert"],
                "comment": row["comment"],
                "size_change": size,
                "event_time": iso(row["event_time"]),
                "stored_after_ms": int(
                    (row["ingested_at"] - row["event_time"]).total_seconds() * 1000
                ),
                "url": page_url(row["server_name"], row["title"]),
            }
        )
    return {"edits": edits, "next_before": edits[-1]["event_time"] if edits else None}


ALERTS = """
    SELECT alert_id, wiki, title, revert_count, users, window_start, window_end, detected_at
    FROM alerts
    ORDER BY window_end DESC
    LIMIT %(limit)s
"""


def alerts(db, limit: int = 50) -> dict:
    rows = db.rows(ALERTS, {"limit": max(1, min(int(limit), 200))})
    return {
        "alerts": [
            {
                "alert_id": r["alert_id"],
                "wiki": r["wiki"],
                "title": r["title"],
                "revert_count": r["revert_count"],
                "users": list(r["users"]),
                "window_start": iso(r["window_start"]),
                "window_end": iso(r["window_end"]),
                "minutes": round((r["window_end"] - r["window_start"]).total_seconds() / 60, 1),
                "detected_at": iso(r["detected_at"]),
            }
            for r in rows
        ]
    }
