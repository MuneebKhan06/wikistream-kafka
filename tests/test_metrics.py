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
