"""Shows what the pipeline has actually produced, straight from PostgreSQL.

Usage:
  python scripts/top_pages.py                 # top pages in the last hour
  python scripts/top_pages.py --hours 6       # a wider window
  python scripts/top_pages.py --wiki enwiki   # one wiki only
  python scripts/top_pages.py --busiest       # busiest minutes instead of pages
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.metrics import setup_logging  # noqa: E402
from sinks.db import connect  # noqa: E402

log = setup_logging("top-pages")

TOP_PAGES = """
    SELECT wiki, title, SUM(edit_count) AS edits, COUNT(*) AS minutes
    FROM trending_minutes
    WHERE minute > NOW() - (%(hours)s * INTERVAL '1 hour')
      AND (%(wiki)s IS NULL OR wiki = %(wiki)s)
    GROUP BY wiki, title
    ORDER BY edits DESC, title
    LIMIT %(limit)s
"""

BUSIEST_MINUTES = """
    SELECT minute, SUM(edit_count) AS edits, COUNT(*) AS pages
    FROM trending_minutes
    WHERE minute > NOW() - (%(hours)s * INTERVAL '1 hour')
      AND (%(wiki)s IS NULL OR wiki = %(wiki)s)
    GROUP BY minute
    ORDER BY minute DESC
    LIMIT %(limit)s
"""

TOTALS = """
    SELECT
        (SELECT COUNT(*) FROM edits) AS stored_edits,
        (SELECT COUNT(*) FROM trending_minutes) AS page_minutes,
        (SELECT COALESCE(MAX(minute), NULL) FROM trending_minutes) AS newest_minute
"""


def rows_for(connection, query: str, params: dict) -> list:
    with connection.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def print_top_pages(rows) -> None:
    if not rows:
        print("no trending rows in this window yet")
        return
    print(f"{'edits':>6}  {'minutes':>7}  {'wiki':<16} title")
    for wiki, title, edits, minutes in rows:
        print(f"{edits:>6}  {minutes:>7}  {wiki:<16} {title[:60]}")


def print_busiest(rows) -> None:
    if not rows:
        print("no trending rows in this window yet")
        return
    print(f"{'minute':<17} {'edits':>6}  {'pages':>6}")
    for minute, edits, pages in rows:
        print(f"{minute:%Y-%m-%d %H:%M}  {edits:>6}  {pages:>6}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=float, default=1.0)
    parser.add_argument("--wiki", default=None)
    parser.add_argument("--limit", type=int, default=15)
    parser.add_argument("--busiest", action="store_true")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    params = {"hours": args.hours, "wiki": args.wiki, "limit": args.limit}
    connection = connect()
    try:
        stored, page_minutes, newest = rows_for(connection, TOTALS, {})[0]
        print(
            f"{stored} edits stored, {page_minutes} page minutes counted"
            + (f", newest minute {newest:%Y-%m-%d %H:%M} UTC" if newest else "")
        )
        print()
        if args.busiest:
            print_busiest(rows_for(connection, BUSIEST_MINUTES, params))
        else:
            print_top_pages(rows_for(connection, TOP_PAGES, params))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
