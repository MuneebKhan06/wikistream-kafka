import logging

from confluent_kafka import TopicPartition

from common.offsets import find_retention_gaps, report_retention_gaps


class FakeConsumer:
    def __init__(self, committed, log_starts):
        self._committed = committed
        self._log_starts = log_starts

    def committed(self, partitions, timeout=None):
        return [
            TopicPartition(tp.topic, tp.partition, self._committed.get(tp.partition, -1001))
            for tp in partitions
        ]

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        return (self._log_starts[tp.partition], 10_000_000)


def partitions(*ids):
    return [TopicPartition("wiki.raw", p) for p in ids]


def test_committed_offset_behind_log_start_is_a_gap():
    consumer = FakeConsumer({0: 100}, {0: 60_000})
    gaps = find_retention_gaps(consumer, partitions(0))
    assert len(gaps) == 1
    assert gaps[0].lost == 59_900


def test_committed_offset_inside_the_log_is_fine():
    consumer = FakeConsumer({0: 70_000}, {0: 60_000})
    assert find_retention_gaps(consumer, partitions(0)) == []


def test_offset_exactly_at_log_start_loses_nothing():
    consumer = FakeConsumer({0: 60_000}, {0: 60_000})
    assert find_retention_gaps(consumer, partitions(0)) == []


def test_a_new_group_without_commits_has_no_gap():
    consumer = FakeConsumer({}, {0: 60_000})
    assert find_retention_gaps(consumer, partitions(0)) == []


def test_report_logs_each_gap_and_totals_the_loss(caplog):
    consumer = FakeConsumer({0: 0, 1: 500, 2: 90_000}, {0: 1_000, 1: 2_000, 2: 60_000})
    with caplog.at_level(logging.ERROR):
        lost = report_retention_gaps(logging.getLogger("t"), consumer, partitions(0, 1, 2))
    assert lost == 1_000 + 1_500
    assert "1000 records expired unread" in caplog.text
    assert "1500 records expired unread" in caplog.text
    assert "wiki.raw[2]" not in caplog.text


def test_no_partitions_means_no_calls():
    assert find_retention_gaps(None, []) == []
