"""Print brokers, controller and per partition leader, replicas and ISR."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka.admin import AdminClient  # noqa: E402

from common.config import admin_config  # noqa: E402


def main() -> None:
    admin = AdminClient(admin_config())
    cluster = admin.describe_cluster().result()
    print(f"Cluster id: {cluster.cluster_id}")
    print(f"Controller: {cluster.controller.id}")
    for node in sorted(cluster.nodes, key=lambda n: n.id):
        print(f"  broker {node.id} at {node.host}:{node.port}")

    metadata = admin.list_topics(timeout=10)
    names = sorted(t for t in metadata.topics if not t.startswith("__"))
    for name in names:
        partitions = metadata.topics[name].partitions
        print(f"\n{name} ({len(partitions)} partitions)")
        print(f"  {'part':>4}  {'leader':>6}  {'replicas':<10}  {'isr':<10}  status")
        for pid in sorted(partitions):
            p = partitions[pid]
            status = "ok" if len(p.isrs) == len(p.replicas) else "UNDER_REPLICATED"
            print(
                f"  {pid:>4}  {p.leader:>6}  {str(p.replicas):<10}  "
                f"{str(p.isrs):<10}  {status}"
            )


if __name__ == "__main__":
    main()
