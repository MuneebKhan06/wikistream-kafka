"""Builds the current state of every page from the compacted topic.

This is what compaction is for: a new consumer reads wiki.page-latest from
the beginning and ends up with the latest edit of every page, without
touching the full history in wiki.clean. A tombstone removes a page.

Also reports how far compaction has got. Until the cleaner runs, older
records for a page are still readable, so records read can be well above
the number of pages. After compaction they converge.

Usage:
  python scripts/page_snapshot.py                        # summary
  python scripts/page_snapshot.py --page "enwiki:Paris"  # one page
  python scripts/page_snapshot.py --wiki dewiki          # one wiki's pages
"""

import argparse
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Consumer, KafkaError, KafkaException  # noqa: E402

from common.config import TOPIC_PAGE_LATEST, consumer_config  # noqa: E402
from common.metrics import setup_logging  # noqa: E402
from processors.page_latest import PageLatest  # noqa: E402

log = setup_logging("page-snapshot")

IDLE_TIMEOUT_SEC = 10.0


def build_snapshot(topic: str = TOPIC_PAGE_LATEST) -> dict:
    """Read the whole topic, applying records and tombstones in order."""
    import uuid

    consumer = Consumer(
        consumer_config(
            "page-snapshot",
            **{
                # A throwaway group: a snapshot always starts from the beginning.
                "group.id": f"page-snapshot-{uuid.uuid4().hex[:8]}",
                "enable.partition.eof": True,
            },
        )
    )
    consumer.subscribe([topic])
    pages = {}
    stats = Counter()
    finished = set()
    metadata = consumer.list_topics(topic, timeout=10)
    partition_count = len(metadata.topics[topic].partitions)

    try:
        while len(finished) < partition_count:
            msg = consumer.poll(IDLE_TIMEOUT_SEC)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    finished.add(msg.partition())
                    continue
                raise KafkaException(msg.error())

            key = msg.key().decode("utf-8") if msg.key() else None
            stats["records"] += 1
            if msg.value() is None:
                stats["tombstones"] += 1
                if pages.pop(key, None) is not None:
                    stats["pages_removed"] += 1
                continue
            if key in pages:
                stats["superseded"] += 1
            pages[key] = PageLatest.from_json(msg.value())
    finally:
        consumer.close()

    stats["pages"] = len(pages)
    return {"pages": pages, "stats": stats}


def print_summary(pages: dict, stats: Counter) -> None:
    print(f"records read      {stats['records']}")
    print(f"live pages        {stats['pages']}")
    print(f"superseded        {stats['superseded']}  (older records compaction will remove)")
    print(f"tombstones        {stats['tombstones']}  ({stats['pages_removed']} pages removed)")
    if stats["pages"]:
        ratio = stats["records"] / stats["pages"]
        print(f"records per page  {ratio:.2f}  (1.00 once fully compacted)")
    print()
    print("pages per wiki:")
    for wiki, count in Counter(p.wiki for p in pages.values()).most_common(8):
        print(f"  {count:>7}  {wiki}")


def print_page(record: PageLatest) -> None:
    print(f"{record.page_key}")
    print(f"  last change   {record.event_time}  ({record.type})")
    print(f"  by            {record.user}{' (bot)' if record.bot else ''}")
    print(f"  revision      {record.rev_id}, {record.length} bytes")
    print(f"  revert        {'yes' if record.is_revert else 'no'}")
    print(f"  comment       {record.comment[:100]}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--page", help='one page, as "wiki:title"')
    parser.add_argument("--wiki", help="list pages from one wiki")
    parser.add_argument("--limit", type=int, default=10)
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    snapshot = build_snapshot()
    pages, stats = snapshot["pages"], snapshot["stats"]

    if args.page:
        record = pages.get(args.page)
        if record is None:
            print(f"{args.page} is not in the snapshot (never edited, or deleted)")
            sys.exit(1)
        print_page(record)
        return

    if args.wiki:
        selected = sorted(
            (p for p in pages.values() if p.wiki == args.wiki),
            key=lambda p: p.event_time,
            reverse=True,
        )
        print(f"{len(selected)} pages from {args.wiki}, most recently changed first:")
        for record in selected[: args.limit]:
            print(f"  {record.event_time[:19]}  {record.title[:60]}")
        return

    print_summary(pages, stats)


if __name__ == "__main__":
    main()
