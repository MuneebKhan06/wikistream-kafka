"""The latest state of every page, kept in memory from the compacted topic.

wiki.page-latest is compacted and keyed by page, so reading it from the
beginning yields one current record per page: Kafka used as a table. A
background thread does that once at startup and then keeps following the
topic, applying each new record and removing a page when a tombstone
arrives. Lookups and searches never touch Kafka or PostgreSQL.

Reads use read_committed, so only what page-state committed is shown. The
consumer assigns partitions directly rather than joining a group: every
dashboard instance needs every partition, and nothing is committed.
"""

import logging
import threading
import time
from typing import Optional

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition

from common.config import TOPIC_PAGE_LATEST, consumer_config, scoped
from processors.page_latest import PageLatest

log = logging.getLogger("web.pages")

POLL_SEC = 0.5
RETRY_SEC = 5.0


class PageStore:
    def __init__(self, topic: str = TOPIC_PAGE_LATEST):
        self.topic = topic
        self._pages = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self.records_read = 0
        self.tombstones = 0
        self.partitions = 0
        self._at_end = set()
        self.error = None

    # Lifecycle.

    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="page-store", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)

    def _run(self) -> None:
        # Kafka may be down when the dashboard starts; keep trying rather than
        # leaving the page view permanently empty.
        while not self._stop.is_set():
            try:
                self._follow()
            except Exception as exc:
                self.error = str(exc)
                log.warning("page store stopped following %s: %s", self.topic, exc)
                self._stop.wait(RETRY_SEC)

    def _follow(self) -> None:
        group = scoped("dashboard-pages")
        consumer = Consumer(
            consumer_config(group, **{"group.id": group, "enable.partition.eof": True})
        )
        try:
            metadata = consumer.list_topics(self.topic, timeout=10).topics[self.topic]
            if metadata.error is not None:
                raise RuntimeError(str(metadata.error))
            self.partitions = len(metadata.partitions)
            with self._lock:
                self._pages.clear()
                self._at_end.clear()
                self.records_read = self.tombstones = 0
            consumer.assign(
                [TopicPartition(self.topic, p, OFFSET_BEGINNING) for p in metadata.partitions]
            )
            self.error = None
            while not self._stop.is_set():
                msg = consumer.poll(POLL_SEC)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        with self._lock:
                            self._at_end.add(msg.partition())
                        continue
                    if msg.error().fatal():
                        raise RuntimeError(msg.error().str())
                    continue
                self.apply(msg.key(), msg.value())
        finally:
            consumer.close()

    # State.

    def apply(self, key: Optional[bytes], value: Optional[bytes]) -> None:
        if key is None:
            return
        page_key = key.decode("utf-8")
        with self._lock:
            self.records_read += 1
            if value is None:
                self.tombstones += 1
                self._pages.pop(page_key, None)
                return
            try:
                self._pages[page_key] = PageLatest.from_json(value)
            except (ValueError, TypeError, KeyError):
                log.warning("unreadable record for %s", page_key)

    def get(self, wiki: str, title: str) -> Optional[PageLatest]:
        with self._lock:
            return self._pages.get(f"{wiki}:{title}")

    def search(self, query: str, wiki: Optional[str] = None, limit: int = 20) -> list:
        """Pages whose title contains the query, most recently changed first."""
        needle = query.casefold().strip()
        with self._lock:
            pages = list(self._pages.values())
        matches = [
            page
            for page in pages
            if (not wiki or page.wiki == wiki) and (not needle or needle in page.title.casefold())
        ]
        matches.sort(key=lambda page: page.event_time, reverse=True)
        return matches[:limit]

    def stats(self) -> dict:
        with self._lock:
            pages = len(self._pages)
            caught_up = self.partitions > 0 and len(self._at_end) >= self.partitions
            return {
                "pages": pages,
                "records_read": self.records_read,
                "tombstones": self.tombstones,
                "records_per_page": round(self.records_read / pages, 2) if pages else None,
                "caught_up": caught_up,
                "error": self.error,
            }

    def wait_until_caught_up(self, timeout: float = 120) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.stats()["caught_up"]:
                return True
            time.sleep(0.2)
        return False
