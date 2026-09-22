"""Producer, consumer and topic settings shared by every component."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094,localhost:9096"
)

HOUR_MS = 60 * 60 * 1000
DAY_MS = 24 * HOUR_MS


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    config: dict


REPLICATION_FACTOR = 3
MIN_INSYNC_REPLICAS = "2"

TOPIC_RAW = "wiki.raw"
TOPIC_CLEAN = "wiki.clean"
TOPIC_ALERTS = "wiki.alerts"
TOPIC_PAGE_LATEST = "wiki.page-latest"
TOPIC_DLQ = "wiki.dlq"

TOPICS = [
    TopicSpec(TOPIC_RAW, 6, {"retention.ms": str(DAY_MS)}),
    TopicSpec(TOPIC_CLEAN, 6, {"retention.ms": str(7 * DAY_MS)}),
    TopicSpec(TOPIC_ALERTS, 3, {"retention.ms": str(30 * DAY_MS)}),
    TopicSpec(
        TOPIC_PAGE_LATEST,
        6,
        {
            "cleanup.policy": "compact",
            "min.cleanable.dirty.ratio": "0.1",
            "segment.ms": str(HOUR_MS),
        },
    ),
    TopicSpec(TOPIC_DLQ, 1, {"retention.ms": str(30 * DAY_MS)}),
]


def producer_config(**overrides) -> dict:
    """Durable producer: acks from all in-sync replicas, no retry duplicates."""
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 20,
        "batch.size": 64 * 1024,
    }
    conf.update(overrides)
    return conf


def transactional_producer_config(transactional_id: str, **overrides) -> dict:
    return producer_config(
        **{"transactional.id": transactional_id, "transaction.timeout.ms": 60000},
        **overrides,
    )


def consumer_config(group_id: str, **overrides) -> dict:
    """Manual commits, read_committed and cooperative rebalancing by default."""
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
        "partition.assignment.strategy": "cooperative-sticky",
    }
    conf.update(overrides)
    return conf


def admin_config() -> dict:
    return {"bootstrap.servers": BOOTSTRAP_SERVERS}


@dataclass(frozen=True)
class PostgresSettings:
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    dbname: str = os.getenv("POSTGRES_DB", "wikistream")
    user: str = os.getenv("POSTGRES_USER", "wikistream")
    password: str = os.getenv("POSTGRES_PASSWORD", "wikistream")

    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password}"
        )


POSTGRES = PostgresSettings()

WIKI_STREAM_URL = os.getenv(
    "WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"
)
