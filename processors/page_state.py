"""Keeps wiki.page-latest holding the latest edit for every page.

The topic is compacted and keyed by page, so it works as a table rather
than a log: Kafka keeps the newest record per page and removes older ones
in the background. A new consumer reads it from the beginning and gets the
current state of every page without replaying the full edit history.

wiki.clean and wiki.page-latest are keyed the same way and have the same
partition count, so each input partition maps to one output partition and
edits to a page are written in the order they happened. Runs on the shared
transactional loop, so read_committed readers never see a partial batch.
"""

import os
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.config import TOPIC_CLEAN, TOPIC_PAGE_LATEST  # noqa: E402
from common.metrics import setup_logging  # noqa: E402
from common.models import CleanEvent  # noqa: E402
from processors.page_latest import page_update  # noqa: E402
from processors.transactional import TransactionalProcessor  # noqa: E402

log = setup_logging("page-state")

GROUP_ID = "page-state"
INSTANCE = os.getenv("PAGE_STATE_INSTANCE", "1")


def transactional_id(instance: str) -> str:
    return f"page-state-{instance}"


class PageState(TransactionalProcessor):
    name = "page state"
    input_topic = TOPIC_CLEAN
    group_id = GROUP_ID

    def __init__(self, instance: str = INSTANCE):
        super().__init__(transactional_id(instance), log, "page-state")
        self.updates = 0
        self.tombstones = 0
        self.ignored = 0

    def handle(self, msg) -> None:
        try:
            event = CleanEvent.from_json(msg.value())
        except (ValueError, TypeError) as exc:
            log.error("unreadable record at offset %d: %s", msg.offset(), exc)
            self.meter.mark_error()
            return

        update = page_update(event)
        if update is None:
            self.ignored += 1
            return

        key, record = update
        _, message_time = msg.timestamp()
        self.producer.produce(
            TOPIC_PAGE_LATEST,
            key=key.encode("utf-8"),
            value=record.to_json() if record is not None else None,
            timestamp=message_time if message_time > 0 else 0,
        )
        if record is None:
            self.tombstones += 1
            log.info("page deleted, tombstone written for %s", key)
        else:
            self.updates += 1
        self.meter.mark()

    def summary(self) -> str:
        return (
            f"{self.updates} page updates, {self.tombstones} tombstones, "
            f"{self.ignored} events not affecting page content"
        )


def main() -> None:
    instance = sys.argv[1] if len(sys.argv) > 1 else INSTANCE
    processor = PageState(instance)
    signal.signal(signal.SIGINT, processor.stop)
    signal.signal(signal.SIGTERM, processor.stop)
    processor.run()


if __name__ == "__main__":
    main()
