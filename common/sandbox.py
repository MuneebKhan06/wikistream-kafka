"""An isolated copy of the pipeline, for tests that run the real processes.

A sandbox has its own namespace, which gives it separate topics, consumer
groups and transactional ids, and its own PostgreSQL database. Processes
started in it are the real ones, pointed at those through the environment.
Nothing it does can reach the running pipeline, and everything it creates is
removed when it closes.

Used by the end to end test and by the failure tests, which need the same
things: start processes, kill them in specific ways, wait for a condition,
and look at what ended up in Kafka and PostgreSQL.
"""

import os
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Callable, Optional

from common.config import PROJECT_ROOT

DEFAULT_TIMEOUT_SEC = 180


def services_available() -> bool:
    import psycopg2
    from confluent_kafka.admin import AdminClient

    from common.config import POSTGRES, admin_config

    try:
        AdminClient(admin_config()).list_topics(timeout=5)
        psycopg2.connect(POSTGRES.dsn(), connect_timeout=5).close()
    except Exception:
        return False
    return True


def admin_connection(dbname: str = "postgres"):
    import psycopg2

    from common.config import POSTGRES

    conn = psycopg2.connect(
        host=POSTGRES.host,
        port=POSTGRES.port,
        user=POSTGRES.user,
        password=POSTGRES.password,
        dbname=dbname,
    )
    conn.autocommit = True
    return conn


def create_database(name: str) -> None:
    conn = admin_connection()
    with conn.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{name}"')
    conn.close()
    conn = admin_connection(name)
    with conn.cursor() as cur:
        cur.execute((PROJECT_ROOT / "db" / "schema.sql").read_text())
    conn.close()


def drop_database(name: str) -> None:
    conn = admin_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s", (name,)
        )
        cur.execute(f'DROP DATABASE IF EXISTS "{name}"')
    conn.close()


def delete_kafka_state(ns: str) -> None:
    """Remove every topic and consumer group belonging to a namespace."""
    from confluent_kafka.admin import AdminClient

    from common.config import admin_config

    admin = AdminClient(admin_config())
    topics = [t for t in admin.list_topics(timeout=10).topics if t.startswith(f"{ns}.")]
    if topics:
        for future in admin.delete_topics(topics).values():
            try:
                future.result()
            except Exception:
                pass
    groups = [
        g.group_id
        for g in admin.list_consumer_groups().result().valid
        if g.group_id.startswith(f"{ns}.")
    ]
    if groups:
        for future in admin.delete_consumer_groups(groups).values():
            try:
                future.result()
            except Exception:
                pass


def produce(topic: str, records) -> None:
    """Produce (key, value) pairs one at a time, in order, each confirmed."""
    from confluent_kafka import Producer

    from common.config import producer_config

    producer = Producer(producer_config())
    for key, value in records:
        producer.produce(topic, key=key, value=value)
        producer.flush(30)
    if producer.flush(30) != 0:
        raise RuntimeError(f"records left undelivered to {topic}")


def committed_offsets(group: str, topic: str) -> dict:
    """{partition: (committed offset or -1 if none, end offset)} for a group."""
    from confluent_kafka import Consumer, TopicPartition

    from common.config import consumer_config

    consumer = Consumer(consumer_config(group, **{"group.id": group}))
    try:
        count = len(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
        parts = [TopicPartition(topic, p) for p in range(count)]
        result = {}
        for tp in consumer.committed(parts, timeout=10):
            _, high = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
            result[tp.partition] = (tp.offset if tp.offset >= 0 else -1, high)
        return result
    finally:
        consumer.close()


def group_caught_up(group: str, topic: str) -> bool:
    """Committed offsets of a group have reached the end of every partition."""
    from confluent_kafka import Consumer, TopicPartition

    from common.config import consumer_config

    consumer = Consumer(consumer_config(group, **{"group.id": group}))
    try:
        count = len(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
        parts = [TopicPartition(topic, p) for p in range(count)]
        for tp in consumer.committed(parts, timeout=10):
            _, high = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
            if tp.offset < 0 and high > 0:
                return False
            if 0 <= tp.offset < high:
                return False
        return True
    finally:
        consumer.close()


class SandboxTimeout(AssertionError):
    pass


class Sandbox:
    def __init__(self, prefix: str = "itest", logs: Optional[Path] = None, database: bool = True):
        self.ns = f"{prefix}{uuid.uuid4().hex[:8]}"
        self.db = f"wikistream_{self.ns}" if database else None
        self.env = dict(os.environ, PIPELINE_NAMESPACE=self.ns)
        if self.db:
            self.env["POSTGRES_DB"] = self.db
        self.logs = Path(logs) if logs is not None else Path(tempfile.mkdtemp(prefix=self.ns))
        self.procs = {}

    def __enter__(self) -> "Sandbox":
        if self.db:
            create_database(self.db)
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def close(self) -> None:
        self.stop_all()
        delete_kafka_state(self.ns)
        if self.db:
            drop_database(self.db)

    # Names inside the namespace.

    def topic(self, name: str) -> str:
        return f"{self.ns}.{name}"

    def group(self, name: str) -> str:
        return f"{self.ns}.{name}"

    # Running things.

    def python(self, script: str, *args, check: bool = True, env: Optional[dict] = None):
        """Run a script to completion."""
        return subprocess.run(
            [sys.executable, str(PROJECT_ROOT / script), *args],
            cwd=PROJECT_ROOT,
            env=dict(self.env, **(env or {})),
            capture_output=True,
            text=True,
            check=check,
            timeout=DEFAULT_TIMEOUT_SEC,
        )

    def start(self, script: str, *args, name: Optional[str] = None, env: Optional[dict] = None):
        """Start a long running process. Returns the name it is tracked under."""
        name = name or script
        log_name = Path(name).stem if name == script else name
        log = open(self.logs / f"{log_name}.log", "a")
        proc = subprocess.Popen(
            [sys.executable, str(PROJECT_ROOT / script), *args],
            cwd=PROJECT_ROOT,
            env=dict(self.env, **(env or {})),
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self.procs[name] = (proc, log)
        return name

    def kill(self, name: str) -> None:
        """SIGKILL: no handler runs, nothing is flushed, like a crash."""
        proc, _ = self.procs[name]
        proc.kill()
        proc.wait(timeout=30)

    def stop(self, name: str, timeout: float = 60) -> int:
        """SIGTERM and wait: the graceful path."""
        proc, _ = self.procs[name]
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
        try:
            return proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            return proc.wait(timeout=30)

    def wait_exit(self, name: str, timeout: float = 120) -> int:
        proc, _ = self.procs[name]
        return proc.wait(timeout=timeout)

    def stop_all(self) -> None:
        for proc, _ in self.procs.values():
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
        for proc, log in self.procs.values():
            try:
                proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                proc.kill()
            if not log.closed:
                log.close()

    def crashed(self, ignore=()) -> list:
        """Tracked processes that exited with an error, unless expected to."""
        return [
            name
            for name, (proc, _) in self.procs.items()
            if name not in ignore and proc.poll() not in (None, 0)
        ]

    # Looking at results.

    def log_text(self, name: str) -> str:
        log_name = Path(name).stem if name.endswith(".py") else name
        path = self.logs / f"{log_name}.log"
        return path.read_text(errors="replace") if path.exists() else ""

    def tails(self, lines: int = 8) -> str:
        out = []
        for path in sorted(self.logs.glob("*.log")):
            tail = path.read_text(errors="replace").splitlines()[-lines:]
            out.append(f"--- {path.name}\n" + "\n".join(tail))
        return "\n".join(out)

    def query(self, sql: str, params=None) -> list:
        conn = admin_connection(self.db)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        finally:
            conn.close()

    def wait_until(
        self,
        description: str,
        condition: Callable[[], bool],
        timeout: float = DEFAULT_TIMEOUT_SEC,
        ignore_exits=(),
        interval: float = 1.0,
    ) -> float:
        """Poll until the condition holds. Returns how long it took."""
        started = time.time()
        while time.time() - started < timeout:
            crashed = self.crashed(ignore=ignore_exits)
            if crashed:
                raise SandboxTimeout(f"{crashed} exited early\n{self.tails()}")
            try:
                if condition():
                    return time.time() - started
            except Exception:
                pass
            time.sleep(interval)
        raise SandboxTimeout(f"timed out waiting for {description}\n{self.tails()}")
