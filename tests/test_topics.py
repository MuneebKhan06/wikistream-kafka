import logging

import pytest

from common.topics import missing_topics, wait_for_topics


class Topic:
    def __init__(self, partitions=1, error=None):
        self.partitions = {p: object() for p in range(partitions)}
        self.error = error


class FakeClient:
    """Topics appear after a number of metadata calls, like a new topic propagating."""

    def __init__(self, appear_after=0):
        self.calls = 0
        self.appear_after = appear_after

    def list_topics(self, timeout=None):
        self.calls += 1
        self.topics = {"wiki.raw": Topic()} if self.calls > self.appear_after else {}
        return self


def test_existing_topics_are_not_missing():
    assert missing_topics(FakeClient(), ["wiki.raw"]) == []


def test_a_topic_with_an_error_or_no_partitions_is_missing():
    client = FakeClient()
    client.list_topics()
    client.list_topics = lambda timeout=None: type(
        "M", (), {"topics": {"a": Topic(error="UNKNOWN"), "b": Topic(partitions=0)}}
    )()
    assert missing_topics(client, ["a", "b"]) == ["a", "b"]


def test_wait_returns_once_a_new_topic_appears(monkeypatch):
    monkeypatch.setattr("common.topics.POLL_SEC", 0)
    client = FakeClient(appear_after=3)
    wait_for_topics(client, ["wiki.raw"], logging.getLogger("t"), wait_sec=5)
    assert client.calls == 4


def test_wait_fails_clearly_when_the_topic_never_appears(monkeypatch):
    monkeypatch.setattr("common.topics.POLL_SEC", 0)
    with pytest.raises(RuntimeError, match="create_topics.py"):
        wait_for_topics(FakeClient(appear_after=10**9), ["wiki.raw"], logging.getLogger("t"), 0.05)


# The watchdog, with a fake group description and topic metadata.

from confluent_kafka import ConsumerGroupState, TopicPartition  # noqa: E402

from common.topics import AssignmentWatchdog  # noqa: E402


class Member:
    def __init__(self, partitions):
        self.assignment = type(
            "A", (), {"topic_partitions": [TopicPartition("wiki.clean", p) for p in partitions]}
        )()


class FakeAdmin:
    def __init__(self, members, state=ConsumerGroupState.STABLE):
        self.members = members
        self.state = state

    def describe_consumer_groups(self, groups):
        description = type("D", (), {"state": self.state, "members": self.members})()
        future = type("F", (), {"result": lambda self: description})()
        return {groups[0]: future}


class Metadata:
    def __init__(self, partitions=6):
        self.topics = {"wiki.clean": Topic(partitions)}


class Consumer:
    def list_topics(self, timeout=None):
        return Metadata()


def watchdog(members, state=ConsumerGroupState.STABLE):
    rejoins = []
    dog = AssignmentWatchdog(
        Consumer(),
        "g",
        ["wiki.clean"],
        logging.getLogger("t"),
        lambda: rejoins.append(1),
        admin=FakeAdmin(members, state),
        interval_sec=0,
    )
    return dog, rejoins


def test_a_fully_owned_topic_is_left_alone():
    dog, rejoins = watchdog([Member([0, 1, 2]), Member([3, 4, 5])])
    for _ in range(3):
        dog.check()
    assert rejoins == []


def test_an_idle_member_is_fine_when_others_own_everything():
    """More members than partitions: an empty assignment is normal."""
    dog, rejoins = watchdog([Member([0, 1, 2, 3, 4, 5]), Member([])])
    for _ in range(3):
        dog.check()
    assert rejoins == []


def test_unowned_partitions_trigger_a_rejoin_on_the_second_check():
    dog, rejoins = watchdog([Member([])])
    dog.check()
    assert rejoins == []
    dog.check()
    assert rejoins == [1]


def test_a_group_mid_rebalance_is_not_judged():
    dog, rejoins = watchdog([Member([])], state=ConsumerGroupState.PREPARING_REBALANCING)
    for _ in range(3):
        dog.check()
    assert rejoins == []


def test_a_gap_that_closes_resets_the_count():
    dog, rejoins = watchdog([Member([])])
    dog.check()
    dog.admin.members = [Member([0, 1, 2, 3, 4, 5])]
    dog.check()
    dog.admin.members = [Member([])]
    dog.check()
    assert rejoins == []


def test_a_failed_check_does_nothing():
    dog, rejoins = watchdog([Member([])])

    def broken(groups):
        raise RuntimeError("coordinator not available")

    dog.admin.describe_consumer_groups = broken
    for _ in range(3):
        dog.check()
    assert rejoins == []
