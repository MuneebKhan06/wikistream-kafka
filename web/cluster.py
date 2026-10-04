"""The health of the cluster and of every consumer group, read from Kafka.

For each pipeline consumer group: its committed offset and the end of the
log on every partition of the topic it reads, the lag between them, the
group's state and how many members it has. For the cluster: which brokers
are up, which is the controller, and whether any partition is without a
leader or short of replicas.

A note on lag of exactly one. Topics written in transactions end each
partition with a commit marker that takes an offset. A consumer that has
read everything commits the offset after the last record, just before that
marker, so an idle group shows one record of lag per partition. That is not
work outstanding, and it is reported as caught up.
"""

import logging
from typing import List, Tuple

from confluent_kafka import ConsumerGroupTopicPartitions, IsolationLevel, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from common.config import (
    NAMESPACE,
    TOPIC_ALERTS,
    TOPIC_CLEAN,
    TOPIC_DLQ,
    TOPIC_RAW,
    scoped,
)

log = logging.getLogger("web.cluster")

TIMEOUT_SEC = 10
EXPECTED_BROKERS = 3

# (group, the topic it reads), in the order data flows through the pipeline.
GROUPS: List[Tuple[str, str]] = [
    (scoped("cleaner"), TOPIC_RAW),
    (scoped("page-state"), TOPIC_CLEAN),
    (scoped("edit-wars"), TOPIC_CLEAN),
    (scoped("trending"), TOPIC_CLEAN),
    (scoped("storage"), TOPIC_CLEAN),
    (scoped("alerts-storage"), TOPIC_ALERTS),
]


def end_offsets(admin: AdminClient, topic: str, partitions: int, spec=None) -> dict:
    spec = spec or OffsetSpec.latest()
    request = {TopicPartition(topic, p): spec for p in range(partitions)}
    futures = admin.list_offsets(
        request, isolation_level=IsolationLevel.READ_COMMITTED, request_timeout=TIMEOUT_SEC
    )
    return {tp.partition: future.result().offset for tp, future in futures.items()}


def group_lag(admin: AdminClient, group: str, topic: str, partitions: int, ends: dict) -> dict:
    request = [ConsumerGroupTopicPartitions(group)]
    committed = {}
    result = admin.list_consumer_group_offsets(request, request_timeout=TIMEOUT_SEC)
    for tp in result[group].result().topic_partitions:
        if tp.topic == topic:
            committed[tp.partition] = tp.offset

    description = admin.describe_consumer_groups([group], request_timeout=TIMEOUT_SEC)[
        group
    ].result()
    per_partition = []
    for partition in range(partitions):
        offset = committed.get(partition, -1)
        end = ends.get(partition, 0)
        lag = end - offset if offset >= 0 else end
        per_partition.append({"partition": partition, "committed": offset, "end": end, "lag": lag})

    total = sum(p["lag"] for p in per_partition)
    caught_up = all(p["lag"] <= 1 for p in per_partition)
    return {
        "group": group,
        "topic": topic,
        "state": description.state.name.lower(),
        "members": len(description.members),
        "lag": total,
        "caught_up": caught_up,
        "partitions": per_partition,
    }


DLQ_COUNT_LIMIT = 100_000


def count_records(topic: str, partitions: int, limit: int = DLQ_COUNT_LIMIT) -> int:
    """Count the records a read_committed reader sees, by reading them.

    Offsets cannot be subtracted for this: the cleaner writes the dead letter
    queue in transactions, and every transaction leaves a commit marker that
    takes an offset of its own, so end minus start counted each record twice.
    The queue is small by design, and the answer is cached by the caller.
    """
    from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError

    from common.config import consumer_config

    group = scoped("dashboard-dlq-count")
    consumer = Consumer(consumer_config(group, **{"group.id": group, "enable.partition.eof": True}))
    count, finished = 0, set()
    try:
        consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in range(partitions)])
        while len(finished) < partitions and count < limit:
            msg = consumer.poll(5)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    finished.add(msg.partition())
                continue
            count += 1
    finally:
        consumer.close()
    return count


def pipeline(admin: AdminClient) -> dict:
    metadata = admin.list_topics(timeout=TIMEOUT_SEC)
    cluster = admin.describe_cluster(request_timeout=TIMEOUT_SEC).result()

    topics = []
    under_replicated = leaderless = 0
    partition_counts = {}
    for name in sorted(metadata.topics):
        if not name.startswith(f"{NAMESPACE}."):
            continue
        topic = metadata.topics[name]
        parts = topic.partitions.values()
        short = sum(1 for p in parts if len(p.isrs) < len(p.replicas))
        offline = sum(1 for p in parts if p.leader < 0)
        under_replicated += short
        leaderless += offline
        partition_counts[name] = len(topic.partitions)
        leaders = {}
        for p in parts:
            leaders[p.leader] = leaders.get(p.leader, 0) + 1
        topics.append(
            {
                "topic": name,
                "partitions": len(topic.partitions),
                "under_replicated": short,
                "leaderless": offline,
                "leaders_by_broker": {str(k): v for k, v in sorted(leaders.items())},
            }
        )

    groups = []
    ends_cache = {}
    for group, topic in GROUPS:
        count = partition_counts.get(topic)
        if not count:
            continue
        if topic not in ends_cache:
            ends_cache[topic] = end_offsets(admin, topic, count)
        try:
            groups.append(group_lag(admin, group, topic, count, ends_cache[topic]))
        except Exception as exc:
            log.warning("could not read group %s: %s", group, exc)
            groups.append({"group": group, "topic": topic, "error": str(exc)})

    dlq_records = None
    if TOPIC_DLQ in partition_counts:
        dlq_records = count_records(TOPIC_DLQ, partition_counts[TOPIC_DLQ])

    brokers = sorted(node.id for node in cluster.nodes)
    return {
        "brokers": {
            "up": brokers,
            "expected": EXPECTED_BROKERS,
            "controller": cluster.controller.id if cluster.controller else None,
        },
        "partitions": {
            "total": sum(partition_counts.values()),
            "under_replicated": under_replicated,
            "leaderless": leaderless,
        },
        "topics": topics,
        "groups": groups,
        "dlq_records": dlq_records,
    }
