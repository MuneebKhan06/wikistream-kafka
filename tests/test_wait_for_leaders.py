from admin.wait_for_leaders import fully_in_sync, led_away_from


class Partition:
    def __init__(self, leader, replicas=(1, 2, 3), isrs=(1, 2, 3)):
        self.leader = leader
        self.replicas = list(replicas)
        self.isrs = list(isrs)


class Topic:
    def __init__(self, partitions):
        self.partitions = dict(enumerate(partitions))


class FakeAdmin:
    def __init__(self, topics):
        self.topics = topics

    def list_topics(self, timeout=None):
        return self


def admin(**topics):
    return FakeAdmin({name.replace("_", "."): Topic(parts) for name, parts in topics.items()})


def test_leadership_has_moved_when_no_partition_is_led_by_the_broker():
    cluster = admin(wiki_raw=[Partition(1), Partition(3)])
    assert led_away_from(cluster, 2) is True


def test_a_partition_still_led_by_the_broker_means_not_yet():
    cluster = admin(wiki_raw=[Partition(1), Partition(2)])
    assert led_away_from(cluster, 2) is False


def test_a_leaderless_partition_means_not_yet():
    cluster = admin(wiki_raw=[Partition(1), Partition(-1)])
    assert led_away_from(cluster, 2) is False


def test_topics_outside_the_pipeline_are_ignored():
    cluster = admin(wiki_raw=[Partition(1)], other_topic=[Partition(2)])
    assert led_away_from(cluster, 2) is True


def test_in_sync_only_when_every_replica_is_in_the_isr():
    assert fully_in_sync(admin(wiki_raw=[Partition(1)])) is True
    lagging = admin(wiki_raw=[Partition(1, isrs=(1, 3))])
    assert fully_in_sync(lagging) is False
