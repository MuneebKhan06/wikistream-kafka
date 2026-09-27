import json

from common.dlq import dlq_record

KEYS = {"stage", "reason", "source", "position", "failed_at", "payload"}


def test_ingestor_and_cleaner_records_share_one_shape():
    ingestor = json.loads(
        dlq_record("ingestor", "missing meta.id", "{}", "https://stream", stream_id="x")
    )
    cleaner = json.loads(
        dlq_record("cleaner", "invalid json", b"{", "wiki.raw", partition=1, offset=2)
    )
    assert set(ingestor) == set(cleaner) == KEYS
    assert ingestor["position"] == {"stream_id": "x"}
    assert cleaner["position"] == {"partition": 1, "offset": 2}


def test_payload_is_kept_verbatim():
    original = '{"meta": {"dt": "2026-09-25T10:15:00Z", "domain": "canary"}}'
    record = json.loads(dlq_record("cleaner", "unknown event type", original, "wiki.raw"))
    assert record["payload"] == original


def test_invalid_utf8_payload_does_not_break_the_record():
    record = json.loads(dlq_record("cleaner", "bad bytes", b"\xff\xfe", "wiki.raw"))
    assert record["payload"]


def test_failed_at_is_utc():
    record = json.loads(dlq_record("ingestor", "x", "", "s"))
    assert record["failed_at"].endswith("+00:00")
