"""Producer, consumer and topic settings shared by every component."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Paths are anchored here rather than to the working directory, so a process
# started by a service manager or cron from elsewhere still finds its state.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = PROJECT_ROOT / "state"
SAMPLES_DIR = PROJECT_ROOT / "samples"

load_dotenv(PROJECT_ROOT / ".env")

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


# Every name the pipeline uses in Kafka hangs off one namespace, so a test run
# can create its own topics, consumer groups and transactional ids next to the
# running pipeline without touching it. Sharing a transactional id would fence
# the real producer, and sharing a group would take its partitions. The
# default namespace keeps the production names unchanged.
DEFAULT_NAMESPACE = "wiki"
NAMESPACE = os.getenv("PIPELINE_NAMESPACE", DEFAULT_NAMESPACE)


def topic_name(name: str) -> str:
    return f"{NAMESPACE}.{name}"


def scoped(name: str) -> str:
    """A consumer group or transactional id, prefixed outside the default namespace."""
    return name if NAMESPACE == DEFAULT_NAMESPACE else f"{NAMESPACE}.{name}"


# How partitions are shared out in a consumer group. Cooperative-sticky moves
# only the partitions that have to move and lets the rest keep processing.
# The eager assignors (range, roundrobin) revoke everything on every
# rebalance; they are selectable so the difference can be measured.
ASSIGNOR = os.getenv("KAFKA_ASSIGNOR", "cooperative-sticky")
COOPERATIVE = ASSIGNOR.startswith("cooperative")

# Three brokers in production. A single broker test cluster sets both to 1.
REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "3"))
MIN_INSYNC_REPLICAS = os.getenv("KAFKA_MIN_INSYNC_REPLICAS", "2")

TOPIC_RAW = topic_name("raw")
TOPIC_CLEAN = topic_name("clean")
TOPIC_ALERTS = topic_name("alerts")
TOPIC_PAGE_LATEST = topic_name("page-latest")
TOPIC_DLQ = topic_name("dlq")

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
        "partition.assignment.strategy": ASSIGNOR,
        # Members learn of a rebalance from a heartbeat response, and a
        # cooperative rebalance takes two rounds, so a partition changing
        # owner is unowned for about one heartbeat interval. Measured with
        # scripts/rebalance_benchmark.py: 3.2 s at the client's 3 s default,
        # 1.05 s at 1 s, and no better at 500 ms.
        "heartbeat.interval.ms": 1000,
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
