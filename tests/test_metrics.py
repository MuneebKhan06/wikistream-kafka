import logging

from common.metrics import RateMeter


def test_rate_meter_reports_on_interval(caplog):
    log = logging.getLogger("test.rate")
    meter = RateMeter(log, "ingestor", interval=0.0)
    with caplog.at_level(logging.INFO):
        meter.mark(5)
    assert meter.total == 5
    assert "ingestor" in caplog.text


def test_rate_meter_stays_quiet_before_interval(caplog):
    log = logging.getLogger("test.rate")
    meter = RateMeter(log, "ingestor", interval=60.0)
    with caplog.at_level(logging.INFO):
        for _ in range(100):
            meter.mark()
    assert meter.total == 100
    assert caplog.text == ""


def test_forced_report_includes_errors(caplog):
    log = logging.getLogger("test.rate")
    meter = RateMeter(log, "cleaner", interval=60.0)
    meter.mark(3)
    meter.mark_error(2)
    with caplog.at_level(logging.INFO):
        meter.report(force=True)
    assert "3 total, 2 errors" in caplog.text


class LagConsumer:
    """Partition 1 fails the way a broker does mid leader change."""

    def __init__(self, failing=(1,)):
        self.failing = set(failing)

    def position(self, tps):
        from confluent_kafka import TopicPartition

        return [TopicPartition(tps[0].topic, tps[0].partition, 90)]

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        from confluent_kafka import KafkaError, KafkaException

        if tp.partition in self.failing:
            raise KafkaException(KafkaError(KafkaError.NOT_LEADER_FOR_PARTITION))
        return (0, 100)

    def committed(self, tps, timeout=None):
        return tps


def test_lag_skips_a_partition_mid_leader_change(caplog):
    from confluent_kafka import TopicPartition

    from common.metrics import log_lag

    parts = [TopicPartition("wiki.clean", p) for p in range(3)]
    with caplog.at_level(logging.INFO):
        total = log_lag(logging.getLogger("t"), LagConsumer(), parts)
    assert total == 20
    assert "unavailable for p1" in caplog.text


def test_lag_reports_nothing_rather_than_crashing_when_all_fail(caplog):
    from confluent_kafka import TopicPartition

    from common.metrics import log_lag

    parts = [TopicPartition("wiki.clean", p) for p in range(2)]
    with caplog.at_level(logging.INFO):
        total = log_lag(logging.getLogger("t"), LagConsumer(failing=(0, 1)), parts)
    assert total == 0
    assert "unavailable for p0, p1" in caplog.text
