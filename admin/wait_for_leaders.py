"""Waits for the cluster to recover from a broker change and reports how long it took.

Polls cluster metadata every 100 ms through the admin client. The Kafka
command line tools each start a JVM, which takes seconds per call and would
swamp the few seconds being measured.

Usage:
  python admin/wait_for_leaders.py away BROKER START_MS [TIMEOUT_SEC]
      until no pipeline partition is led by BROKER and none is leaderless
  python admin/wait_for_leaders.py insync START_MS [TIMEOUT_SEC]
      until no pipeline partition is under-replicated

START_MS is the epoch time in milliseconds when the change was made, so
the time this script takes to start is counted too.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka.admin import AdminClient  # noqa: E402

from common.config import NAMESPACE, admin_config  # noqa: E402

POLL_SEC = 0.1
PREFIX = f"{NAMESPACE}."


def pipeline_partitions(admin: AdminClient):
    metadata = admin.list_topics(timeout=2)
    for name, topic in metadata.topics.items():
        if name.startswith(PREFIX):
            yield from topic.partitions.values()


def led_away_from(admin: AdminClient, broker: int) -> bool:
    return all(p.leader not in (broker, -1) for p in pipeline_partitions(admin))


def fully_in_sync(admin: AdminClient) -> bool:
    return all(len(p.isrs) == len(p.replicas) for p in pipeline_partitions(admin))


def wait(check, start_ms: int, timeout_sec: float) -> int:
    admin = AdminClient(admin_config())
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            if check(admin):
                return int(time.time() * 1000) - start_ms
        except Exception:
            # Metadata can briefly fail while the cluster is changing.
            pass
        time.sleep(POLL_SEC)
    raise TimeoutError(f"not recovered within {timeout_sec:.0f}s")


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    try:
        if mode == "away":
            broker, start_ms = int(sys.argv[2]), int(sys.argv[3])
            timeout = float(sys.argv[4]) if len(sys.argv) > 4 else 90
            elapsed = wait(lambda a: led_away_from(a, broker), start_ms, timeout)
        elif mode == "insync":
            start_ms = int(sys.argv[2])
            timeout = float(sys.argv[3]) if len(sys.argv) > 3 else 90
            elapsed = wait(fully_in_sync, start_ms, timeout)
        else:
            print(__doc__)
            sys.exit(2)
    except TimeoutError as exc:
        print(exc)
        sys.exit(1)
    print(elapsed)


if __name__ == "__main__":
    main()
