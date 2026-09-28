"""Reads wiki.raw, deduplicates and cleans, writes wiki.clean.

Runs on the shared transactional loop, so cleaned output and input offsets
commit together. Set CLEANER_INSTANCE or pass it as the first argument to
give each instance its own transactional id.

Dedup state is per partition and lives in memory. A partition that moves to
another instance takes its state with it, so revoked partitions are cleared
on rebalance rather than kept around stale.
"""

import os
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.config import TOPIC_CLEAN, TOPIC_DLQ, TOPIC_RAW  # noqa: E402
from common.metrics import setup_logging  # noqa: E402
from processors.dedup import DedupCache  # noqa: E402
from processors.transactional import TransactionalProcessor  # noqa: E402
from processors.transform import Clean, Duplicate, Rejected, transform  # noqa: E402
from processors.warmup import CacheWarmer  # noqa: E402

log = setup_logging("cleaner")

GROUP_ID = "cleaner"
INSTANCE = os.getenv("CLEANER_INSTANCE", "1")


def transactional_id(instance: str) -> str:
    return f"cleaner-{instance}"


class Cleaner(TransactionalProcessor):
    name = "cleaner"
    input_topic = TOPIC_RAW
    group_id = GROUP_ID

    def __init__(self, instance: str = INSTANCE, warmer: CacheWarmer = None):
        super().__init__(transactional_id(instance), log, "clean")
        self.warmer = warmer if warmer is not None else CacheWarmer()
        self.caches = {}
        self.produced = 0
        self.duplicates = 0
        self.rejected = 0

    def partitions_assigned(self, consumer, partitions) -> None:
        self.warm(consumer, partitions)

    def warm(self, consumer, partitions) -> None:
        """Rebuild dedup state for new partitions before processing them.

        The replay stops at each partition's committed offset, which is also
        where processing resumes, so events the group has not yet committed
        stay unknown to the cache and are cleaned normally.
        """
        if not partitions:
            return
        committed = {
            tp.partition: tp.offset
            for tp in consumer.committed(list(partitions), timeout=30)
        }
        for tp in partitions:
            cache = self.caches.setdefault(tp.partition, DedupCache())
            offset = committed.get(tp.partition, -1)
            if offset is None or offset < 0:
                log.info(
                    "partition %d has no committed offset, skipping warmup",
                    tp.partition,
                )
                continue
            result = self.warmer.warm(tp.partition, cache, offset)
            if result.replayed:
                log.info(
                    "warmed partition %d with %d ids from offsets %d to %d in %.1fs",
                    tp.partition,
                    result.loaded,
                    result.from_offset,
                    result.until_offset - 1,
                    result.seconds,
                )

    def partitions_revoked(self, partitions) -> None:
        """A revoked partition may be processed elsewhere, so drop its state."""
        for tp in partitions:
            cache = self.caches.pop(tp.partition, None)
            if cache:
                cache.clear()

    def cache_for(self, partition: int) -> DedupCache:
        return self.caches.setdefault(partition, DedupCache())

    def handle(self, msg) -> None:
        cache = self.cache_for(msg.partition())
        _, message_time = msg.timestamp()
        result = transform(msg.value(), cache, message_time_ms=message_time)

        if isinstance(result, Clean):
            self.producer.produce(
                TOPIC_CLEAN,
                key=result.key.encode("utf-8"),
                value=result.event.to_json(),
                timestamp=message_time if message_time > 0 else 0,
            )
            self.produced += 1
            self.meter.mark()
        elif isinstance(result, Duplicate):
            self.duplicates += 1
        elif isinstance(result, Rejected):
            self.rejected += 1
            self.meter.mark_error()
            log.warning("rejected offset %d: %s", msg.offset(), result.reason)
            self.producer.produce(
                TOPIC_DLQ,
                value=result.envelope(msg.topic(), msg.partition(), msg.offset()),
            )

    def summary(self) -> str:
        return (
            f"{self.produced} cleaned, {self.duplicates} duplicates dropped, "
            f"{self.rejected} rejected"
        )


def main() -> None:
    instance = sys.argv[1] if len(sys.argv) > 1 else INSTANCE
    cleaner = Cleaner(instance)
    signal.signal(signal.SIGINT, cleaner.stop)
    signal.signal(signal.SIGTERM, cleaner.stop)
    cleaner.run()


if __name__ == "__main__":
    main()
