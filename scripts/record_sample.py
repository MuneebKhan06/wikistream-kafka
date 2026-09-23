"""Records live events to a jsonl file so tests and replays are deterministic.

Usage: python scripts/record_sample.py [count] [output]
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.config import WIKI_STREAM_URL  # noqa: E402
from common.metrics import setup_logging  # noqa: E402
from common.models import ParseError, clean_event, parse_raw  # noqa: E402
from ingestor.sse import stream_events  # noqa: E402

log = setup_logging("record-sample")

DEFAULT_COUNT = 2000
DEFAULT_OUTPUT = Path("samples/recentchange_sample.jsonl")


def record(count: int, output: Path) -> dict:
    stats = {"written": 0, "unparseable": 0, "reverts": 0, "bots": 0}
    wikis = set()
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", encoding="utf-8") as handle:
        for event in stream_events(WIKI_STREAM_URL, log=log):
            if event.event != "message":
                continue
            try:
                raw = parse_raw(event.data)
                parsed = clean_event(raw)
            except ParseError as exc:
                stats["unparseable"] += 1
                log.debug("skipped: %s", exc)
                continue

            handle.write(json.dumps(raw, ensure_ascii=False) + "\n")
            stats["written"] += 1
            stats["reverts"] += int(parsed.is_revert)
            stats["bots"] += int(parsed.bot)
            wikis.add(parsed.wiki)

            if stats["written"] % 250 == 0:
                log.info("%d / %d events", stats["written"], count)
            if stats["written"] >= count:
                break

    stats["wikis"] = len(wikis)
    return stats


def main() -> None:
    count = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_COUNT
    output = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_OUTPUT
    stats = record(count, output)
    log.info(
        "wrote %d events from %d wikis to %s (%d reverts, %d bot edits, %d skipped)",
        stats["written"],
        stats["wikis"],
        output,
        stats["reverts"],
        stats["bots"],
        stats["unparseable"],
    )


if __name__ == "__main__":
    main()
