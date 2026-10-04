"""Reads the live Wikimedia recentchange stream into wiki.raw.

Ordering note: messages are produced across 6 partitions, so delivery
callbacks do not arrive in produce order. The checkpoint may only move to
an event whose predecessors are all confirmed, otherwise a crash would
skip the gaps. DeliveryTracker keeps produce order and releases the
longest confirmed prefix.

A failed delivery stops the ingestor. The producer has already retried
until its delivery timeout, so Kafka is refusing writes, for example with
two of three brokers down. Carrying on would leave that event missing from
Kafka, hold the checkpoint behind it forever, and grow the list of pending
deliveries without limit. Stopping instead keeps the checkpoint before the
failed event, so the next start re-reads it and nothing is lost. Events
after it that did get through arrive twice, and the cleaner removes them.
"""

import json
import signal
import sys
import time
from collections import deque
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests  # noqa: E402
from confluent_kafka import KafkaException, Producer  # noqa: E402

from common.config import (  # noqa: E402
    TOPIC_DLQ,
    TOPIC_RAW,
    WIKI_STREAM_URL,
    producer_config,
)
from common.dlq import dlq_record  # noqa: E402
from common.metrics import RateMeter, setup_logging  # noqa: E402
from common.models import ParseError, event_id_of, parse_raw  # noqa: E402
from ingestor.checkpoint import DEFAULT_PATH, Checkpoint  # noqa: E402
from ingestor.sse import stream_events  # noqa: E402

log = setup_logging("ingestor")

SAVE_EVERY = 200
SAVE_INTERVAL_SEC = 5.0
MAX_BACKOFF_SEC = 60.0
QUEUE_FULL_GIVE_UP_SEC = 60.0
QUEUE_FULL_POLL_SEC = 0.5


class DeliveryTracker:
    """Releases stream ids in produce order once Kafka confirms them."""

    def __init__(self):
        self._pending = deque()
        self._confirmed = set()

    def register(self, seq: int, stream_id: Optional[str]) -> None:
        self._pending.append((seq, stream_id))

    def confirm(self, seq: int) -> tuple:
        """Mark one message delivered.

        Returns the newest id now safe to save and how many messages the
        release covered, which can be more than one when an older message
        was the last to be confirmed.
        """
        self._confirmed.add(seq)
        released = None
        count = 0
        while self._pending and self._pending[0][0] in self._confirmed:
            seq_done, stream_id = self._pending.popleft()
            self._confirmed.discard(seq_done)
            count += 1
            if stream_id:
                released = stream_id
        return released, count

    @property
    def in_flight(self) -> int:
        return len(self._pending)


class Ingestor:
    def __init__(self, checkpoint_path: Path = DEFAULT_PATH):
        self.checkpoint = Checkpoint.load(checkpoint_path)
        self.producer = Producer(producer_config())
        self.tracker = DeliveryTracker()
        self.meter = RateMeter(log, "ingest")
        self.running = True
        self.failure = None
        self.seq = 0
        self._unsaved = 0
        self._last_save = time.monotonic()

    def stop(self, *_):
        log.info("stop requested, draining")
        self.running = False

    def _on_delivery(self, err, msg, seq: int, stream_id: Optional[str]):
        if err is not None:
            # Never confirmed, so the checkpoint cannot move past this event.
            self.meter.mark_error()
            if self.failure is None:
                self.failure = err
                log.error("delivery failed, stopping so no event is skipped: %s", err)
            self.running = False
            return
        released, count = self.tracker.confirm(seq)
        if count:
            self.checkpoint.advance(released, count)
            self._unsaved += count
        self.meter.mark()
        self._maybe_save(msg)

    def _maybe_save(self, _msg=None):
        now = time.monotonic()
        due = self._unsaved >= SAVE_EVERY or now - self._last_save >= SAVE_INTERVAL_SEC
        if self._unsaved and due:
            self.checkpoint.save()
            self._unsaved = 0
            self._last_save = now

    def _send(self, topic: str, key: Optional[str], value: str, stream_id) -> bool:
        """Hand one message to the producer. Returns False if the ingestor gave up.

        A full local queue means deliveries are not completing, usually because
        the brokers are unreachable. Callbacks are served to make room and the
        send is retried; if the queue stays full, the ingestor stops the same
        way it does on a failed delivery, so the checkpoint stays before this
        event and the next start reads it again.

        The message joins the delivery tracker only once produce() has
        accepted it. Registered first, a send that never happened would hold
        the checkpoint behind it for good.
        """
        seq = self.seq + 1
        waited_since = None
        while True:
            try:
                self.producer.produce(
                    topic,
                    key=key.encode("utf-8") if key else None,
                    value=value.encode("utf-8"),
                    on_delivery=lambda err, msg, s=seq, sid=stream_id: self._on_delivery(
                        err, msg, s, sid
                    ),
                )
                break
            except BufferError:
                now = time.monotonic()
                waited_since = waited_since or now
                if now - waited_since >= QUEUE_FULL_GIVE_UP_SEC or not self.running:
                    if self.failure is None:
                        self.failure = "local queue full: Kafka is not accepting writes"
                        log.error(
                            "producer queue stayed full for %.0fs, stopping so no event is skipped",
                            now - waited_since,
                        )
                    self.running = False
                    return False
                self.producer.poll(QUEUE_FULL_POLL_SEC)
        self.seq = seq
        self.tracker.register(seq, stream_id)
        return True

    def handle(self, event) -> None:
        if event.event == "error":
            log.warning("stream reported an error event: %s", event.data[:200])
            return
        try:
            raw = parse_raw(event.data)
            key = event_id_of(raw)
        except ParseError as exc:
            log.warning("unparseable event sent to dlq: %s", exc)
            self.meter.mark_error()
            record = dlq_record(
                "ingestor", str(exc), event.data, WIKI_STREAM_URL, stream_id=event.event_id
            )
            self._send(TOPIC_DLQ, None, record.decode("utf-8"), event.event_id)
            return
        self._send(TOPIC_RAW, key, json.dumps(raw, ensure_ascii=False), event.event_id)

    def run(self) -> None:
        if self.checkpoint.last_event_id:
            log.info("resuming after %d confirmed events", self.checkpoint.events_confirmed)
        backoff = 1.0

        while self.running:
            try:
                for event in stream_events(
                    WIKI_STREAM_URL, headers=self.checkpoint.resume_header(), log=log
                ):
                    self.handle(event)
                    self.producer.poll(0)
                    backoff = 1.0
                    if not self.running:
                        break
                if self.running:
                    log.warning("stream closed, reconnecting")
            except (requests.RequestException, KafkaException) as exc:
                log.warning("connection lost (%s), retry in %.0fs", exc, backoff)
                self.producer.poll(0)
                if not self._sleep(backoff):
                    break
                backoff = min(backoff * 2, MAX_BACKOFF_SEC)

        self.shutdown()

    def _sleep(self, seconds: float) -> bool:
        deadline = time.monotonic() + seconds
        while self.running and time.monotonic() < deadline:
            self.producer.poll(0.2)
        return self.running

    def shutdown(self) -> None:
        remaining = self.producer.flush(30)
        if remaining:
            log.error("%d messages still undelivered after flush", remaining)
        if self._unsaved:
            self.checkpoint.save()
        self.meter.report(force=True)
        log.info(
            "stopped, %d events confirmed in total, %d in flight",
            self.checkpoint.events_confirmed,
            self.tracker.in_flight,
        )


def main() -> None:
    ingestor = Ingestor()
    signal.signal(signal.SIGINT, ingestor.stop)
    signal.signal(signal.SIGTERM, ingestor.stop)
    ingestor.run()
    if ingestor.failure is not None:
        # Non zero so a supervisor restarts it from the checkpoint.
        sys.exit(1)


if __name__ == "__main__":
    main()
