"""Fills in edits.is_revert for rows stored before the column existed.

Uses the same revert detector the pipeline applies to new edits, so old and
new rows agree. Runs in batches and only touches rows still NULL, so it can
be stopped and run again at any time.

Usage: python admin/backfill_reverts.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from psycopg2.extras import execute_values  # noqa: E402

from common.metrics import setup_logging  # noqa: E402
from common.models import is_revert_comment  # noqa: E402
from sinks.db import connect  # noqa: E402

log = setup_logging("backfill-reverts")

BATCH = 5000

SELECT_BATCH = """
    SELECT event_id, type, comment FROM edits
    WHERE is_revert IS NULL
    LIMIT %s
"""

UPDATE_BATCH = """
    UPDATE edits SET is_revert = v.is_revert
    FROM (VALUES %s) AS v(event_id, is_revert)
    WHERE edits.event_id = v.event_id
"""


def main() -> None:
    conn = connect()
    updated = reverts = 0
    try:
        while True:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(SELECT_BATCH, (BATCH,))
                    rows = cur.fetchall()
                    if not rows:
                        break
                    values = [
                        (event_id, kind == "edit" and is_revert_comment(comment))
                        for event_id, kind, comment in rows
                    ]
                    execute_values(cur, UPDATE_BATCH, values, page_size=BATCH)
            updated += len(values)
            reverts += sum(1 for _, flag in values if flag)
            if updated % 50000 < BATCH:
                log.info("%d rows done, %d reverts so far", updated, reverts)
    finally:
        conn.close()
    log.info("backfill complete: %d rows updated, %d of them reverts", updated, reverts)


if __name__ == "__main__":
    main()
