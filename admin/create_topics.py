"""Create every pipeline topic from common.config.TOPICS.

Existing topics are left alone, but their config is brought in line with
the spec so the definitions in code stay the source of truth.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import KafkaException  # noqa: E402
from confluent_kafka.admin import (  # noqa: E402
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
    ResourceType,
)

from common.config import (  # noqa: E402
    MIN_INSYNC_REPLICAS,
    REPLICATION_FACTOR,
    TOPICS,
    admin_config,
)


def topic_config(spec) -> dict:
    return {"min.insync.replicas": MIN_INSYNC_REPLICAS, **spec.config}


def create_missing(admin: AdminClient, existing: set) -> None:
    new_topics = [
        NewTopic(
            spec.name,
            num_partitions=spec.partitions,
            replication_factor=REPLICATION_FACTOR,
            config=topic_config(spec),
        )
        for spec in TOPICS
        if spec.name not in existing
    ]
    if not new_topics:
        print("All topics already exist")
        return

    for name, future in admin.create_topics(new_topics).items():
        try:
            future.result()
            print(f"Created {name}")
        except KafkaException as exc:
            print(f"Failed to create {name}: {exc}")


def sync_configs(admin: AdminClient, existing: set) -> None:
    resources = []
    for spec in TOPICS:
        if spec.name not in existing:
            continue
        entries = [
            ConfigEntry(key, value, incremental_operation=AlterConfigOpType.SET)
            for key, value in topic_config(spec).items()
        ]
        resources.append(
            ConfigResource(ResourceType.TOPIC, spec.name, incremental_configs=entries)
        )
    if not resources:
        return

    for resource, future in admin.incremental_alter_configs(resources).items():
        future.result()
        print(f"Config synced for {resource.name}")


def check_partitions(admin: AdminClient) -> None:
    metadata = admin.list_topics(timeout=10).topics
    for spec in TOPICS:
        if spec.name not in metadata:
            continue
        actual = len(metadata[spec.name].partitions)
        if actual != spec.partitions:
            print(
                f"Warning: {spec.name} has {actual} partitions, spec says "
                f"{spec.partitions}. Partition changes are not applied automatically."
            )


def main() -> None:
    admin = AdminClient(admin_config())
    existing = set(admin.list_topics(timeout=10).topics)
    create_missing(admin, existing)
    sync_configs(admin, existing)
    check_partitions(admin)


if __name__ == "__main__":
    main()
