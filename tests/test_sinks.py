from processors.edit_war_detector import EditWarAlert
from sinks.alerts_sink import GROUP_ID
from sinks.db import insert_alerts


def test_alerts_sink_is_wired_to_the_alerts_topic(monkeypatch):
    import sinks.postgres_sink as postgres_sink

    captured = {}

    class FakeSink:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr("sinks.alerts_sink.PostgresSink", FakeSink)
    from sinks import alerts_sink

    alerts_sink.build()
    assert captured["topic"] == "wiki.alerts"
    assert captured["group_id"] == GROUP_ID
    assert captured["decode"] == EditWarAlert.from_json
    assert captured["write"] is insert_alerts
    assert postgres_sink.GROUP_ID != GROUP_ID
