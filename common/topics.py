"""Startup and runtime handling for topic availability.

Topics are created from code and auto creation is off, so a consumer can
start before its input exists. Each consumer waits for its input topics
before subscribing, and fails clearly if they never appear.

At runtime the client reports some conditions as errors it is already
retrying, such as a topic briefly unknown during metadata propagation. Only
errors the client marks fatal should stop a process.

Waiting is not quite enough. When a group forms right after its topic was
created, the member elected leader fetches metadata from the group
coordinator to compute the assignment, and that broker may not know the
topic yet. The assignment then covers zero partitions. The client never
notices: its metadata refreshes compare against the subscription, which
already knew the partition count, not against what was assigned. Seen in
roughly one in ten fresh subscriptions on this cluster, with the partitions
left unowned until something else triggered a rebalance. AssignmentWatchdog
catches that state and forces the group to rejoin.

Measured over 30 fresh subscriptions each: without the watchdog 5 stayed
stuck for the full 60 s observed; with it, the 2 that got stuck owned every
partition 6.5 s later. In practice its first check does the work: listing
all topics' metadata makes the client notice the mismatch and rejoin by
itself. The explicit rejoin after a second failed check is the backstop.
"""

import logging
import time
from typing import Callable, Iterable

from confluent_kafka import ConsumerGroupState, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

from common.config import COOPERATIVE, admin_config

WAIT_SEC = 60.0
POLL_SEC = 1.0


def missing_topics(client, topics: Iterable[str]) -> list:
    metadata = client.list_topics(timeout=10)
    missing = []
    for name in topics:
        topic = metadata.topics.get(name)
        if topic is None or topic.error is not None or not topic.partitions:
            missing.append(name)
    return missing


def wait_for_topics(client, topics: Iterable[str], log: logging.Logger, wait_sec=WAIT_SEC):
    topics = list(topics)
    deadline = time.monotonic() + wait_sec
    missing = missing_topics(client, topics)
    if missing:
        log.info("waiting for %s to exist", ", ".join(missing))
    while missing:
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"{', '.join(missing)} still missing after {wait_sec:.0f}s. "
                "Topics are not created automatically: run admin/create_topics.py."
            )
        time.sleep(POLL_SEC)
        missing = missing_topics(client, topics)


def take_partitions(consumer, partitions) -> None:
    """Accept an assignment in whichever rebalance protocol is in use.

    Cooperative rebalances hand over only the partitions being added, so they
    are added to what the consumer holds. Eager rebalances hand over the full
    new assignment, which replaces it.
    """
    if COOPERATIVE:
        consumer.incremental_assign(partitions)
    else:
        consumer.assign(partitions)


def release_partitions(consumer, partitions) -> None:
    """Give partitions up: just these when cooperative, everything when eager."""
    if COOPERATIVE:
        consumer.incremental_unassign(partitions)
    else:
        consumer.unassign()


def check_consumer_error(msg, log: logging.Logger) -> None:
    """Raise for fatal consumer errors; log and carry on for the rest."""
    error = msg.error()
    if error.code() == KafkaError._PARTITION_EOF:
        return
    if error.fatal():
        raise KafkaException(error)
    log.warning("consumer error, client is retrying: %s", error.str())


WATCHDOG_INTERVAL_SEC = 15.0
STRIKES_BEFORE_REJOIN = 2


class AssignmentWatchdog:
    """Forces a rejoin when partitions of the subscribed topics have no owner.

    Looks at the whole group, not just this member: with more members than
    partitions, an empty assignment for one member is normal. Only a stable
    group is judged, since mid rebalance partitions are briefly unowned, and
    the gap has to show on two checks in a row before anything is done.
    """

    def __init__(
        self,
        consumer,
        group_id: str,
        topics: Iterable[str],
        log: logging.Logger,
        rejoin: Callable[[], None],
        admin=None,
        interval_sec: float = WATCHDOG_INTERVAL_SEC,
    ):
        self.consumer = consumer
        self.group_id = group_id
        self.topics = set(topics)
        self.log = log
        self.rejoin = rejoin
        self.admin = admin if admin is not None else AdminClient(admin_config())
        self.interval_sec = interval_sec
        self.strikes = 0
        self.rejoins = 0
        self._last_check = time.monotonic()

    def unowned_partitions(self) -> int:
        """Partitions of the subscribed topics no member holds; 0 if unsure."""
        description = self.admin.describe_consumer_groups([self.group_id])[
            self.group_id
        ].result()
        if description.state != ConsumerGroupState.STABLE:
            return 0
        owned = {
            (tp.topic, tp.partition)
            for member in description.members
            for tp in member.assignment.topic_partitions
            if tp.topic in self.topics
        }
        metadata = self.consumer.list_topics(timeout=10)
        total = 0
        for name in self.topics:
            topic = metadata.topics.get(name)
            if topic is not None and topic.error is None:
                total += len(topic.partitions)
        return max(total - len(owned), 0)

    def check(self) -> None:
        now = time.monotonic()
        if now - self._last_check < self.interval_sec:
            return
        self._last_check = now
        try:
            unowned = self.unowned_partitions()
        except Exception as exc:
            self.log.debug("assignment check skipped: %s", exc)
            return

        if unowned == 0:
            self.strikes = 0
            return
        self.strikes += 1
        if self.strikes < STRIKES_BEFORE_REJOIN:
            return
        self.log.warning(
            "%d partitions of %s have no owner in group %s, rejoining",
            unowned,
            ", ".join(sorted(self.topics)),
            self.group_id,
        )
        self.strikes = 0
        self.rejoins += 1
        self.rejoin()
