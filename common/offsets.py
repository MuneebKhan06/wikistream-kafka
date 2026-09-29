"""Detects committed offsets that fell behind the topic's retention.

If a consumer group stays away longer than a topic's retention, the records
at its committed offset are deleted before it reads them. On return, Kafka
finds the offset out of range and auto.offset.reset quietly moves the group
to the oldest record still kept. Nothing fails and nothing is logged, yet
every record in between is gone.

Those records cannot be recovered, but the loss must not be silent. Each
consumer checks its committed offsets against the log start on assignment
and reports exactly how many records expired unread.
"""

import logging
from dataclasses import dataclass
from typing import Iterable, List

from confluent_kafka import TopicPartition


@dataclass(frozen=True)
class RetentionGap:
    topic: str
    partition: int
    committed: int
    log_start: int

    @property
    def lost(self) -> int:
        return self.log_start - self.committed


def find_retention_gaps(consumer, partitions: Iterable[TopicPartition]) -> List[RetentionGap]:
    partitions = list(partitions)
    if not partitions:
        return []
    gaps = []
    for tp in consumer.committed(partitions, timeout=30):
        if tp.offset is None or tp.offset < 0:
            # No commit yet: a new group, nothing to have lost.
            continue
        log_start, _ = consumer.get_watermark_offsets(
            TopicPartition(tp.topic, tp.partition), timeout=10, cached=False
        )
        if tp.offset < log_start:
            gaps.append(RetentionGap(tp.topic, tp.partition, tp.offset, log_start))
    return gaps


def report_retention_gaps(
    log: logging.Logger, consumer, partitions: Iterable[TopicPartition]
) -> int:
    """Log every gap as an error and return the total records lost."""
    gaps = find_retention_gaps(consumer, partitions)
    for gap in gaps:
        log.error(
            "%s[%d]: committed offset %d is before the log start %d, so %d records "
            "expired unread. This group was away longer than the topic's retention.",
            gap.topic,
            gap.partition,
            gap.committed,
            gap.log_start,
            gap.lost,
        )
    return sum(gap.lost for gap in gaps)
