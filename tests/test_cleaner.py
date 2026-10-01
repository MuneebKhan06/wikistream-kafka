import json

from processors.dedup import DedupCache
from processors.transform import Clean, Duplicate, Rejected, transform

RAW = {
    "meta": {"id": "e-1", "dt": "2026-09-23T10:00:00Z"},
    "type": "edit",
    "namespace": 0,
    "title": "Roma",
    "comment": "fix typo",
    "timestamp": 1790000000,
    "user": "Alice",
    "bot": False,
    "wiki": "itwiki",
    "server_name": "it.wikipedia.org",
}


def payload(**overrides):
    raw = json.loads(json.dumps(RAW))
    raw.update(overrides)
    return json.dumps(raw).encode("utf-8")


def test_valid_event_is_cleaned_and_keyed_by_page():
    result = transform(payload(), DedupCache())
    assert isinstance(result, Clean)
    assert result.key == "itwiki:Roma"
    assert result.event.event_id == "e-1"


def test_repeat_of_the_same_event_is_a_duplicate():
    cache = DedupCache()
    assert isinstance(transform(payload(), cache), Clean)
    result = transform(payload(), cache)
    assert isinstance(result, Duplicate)
    assert result.event_id == "e-1"


def test_two_different_events_both_pass():
    cache = DedupCache()
    transform(payload(), cache)
    other = payload(meta={"id": "e-2", "dt": "2026-09-23T10:00:01Z"})
    assert isinstance(transform(other, cache), Clean)


def test_unparseable_payload_is_rejected():
    result = transform(b"{not json", DedupCache())
    assert isinstance(result, Rejected)
    assert "invalid json" in result.reason


def test_valid_json_with_bad_fields_is_rejected():
    result = transform(payload(type="nonsense"), DedupCache())
    assert isinstance(result, Rejected)
    assert "unknown event type" in result.reason


def test_dlq_envelope_keeps_payload_and_position():
    result = transform(b"{not json", DedupCache())
    envelope = json.loads(result.envelope("wiki.raw", 3, 4242))
    assert envelope["stage"] == "cleaner"
    assert envelope["position"] == {"partition": 3, "offset": 4242}
    assert envelope["payload"] == "{not json"
    assert envelope["source"] == "wiki.raw"
    assert "invalid json" in envelope["reason"]


def test_dlq_envelope_survives_invalid_utf8():
    result = transform(b"\xff\xfe", DedupCache())
    assert isinstance(result, Rejected)
    envelope = json.loads(result.envelope("wiki.raw", 0, 1))
    assert envelope["payload"]


def test_message_timestamp_is_used_when_event_time_is_unusable():
    cache = DedupCache()
    raw = json.loads(payload())
    raw["meta"] = {"id": "e-9"}
    del raw["timestamp"]
    result = transform(json.dumps(raw).encode(), cache, message_time_ms=1)
    assert isinstance(result, Rejected)


def test_transactional_id_is_per_instance():
    from processors.cleaner import transactional_id

    assert transactional_id("1") == "cleaner-1"
    assert transactional_id("2") != transactional_id("1")


CANARY = {
    "$schema": "/mediawiki/recentchange/1.0.1",
    "meta": {
        "dt": "2026-09-25T10:15:00.000Z",
        "stream": "mediawiki.recentchange",
        "domain": "canary",
        "id": "bd9854de-9eda-4c4e-be9f-dfc5ef94795b",
    },
}


def test_stream_heartbeats_are_skipped_not_rejected():
    from processors.transform import Skipped

    result = transform(json.dumps(CANARY).encode(), DedupCache())
    assert isinstance(result, Skipped)
    assert result.reason == "canary"


def test_a_heartbeat_does_not_enter_the_dedup_cache():
    cache = DedupCache()
    transform(json.dumps(CANARY).encode(), cache)
    assert cache.size == 0


def test_a_real_event_without_a_type_is_still_rejected():
    """Only the canary marker exempts an event, not a missing type."""
    raw = json.loads(payload())
    del raw["type"]
    result = transform(json.dumps(raw).encode(), DedupCache())
    assert isinstance(result, Rejected)


def test_canary_detection_tolerates_odd_meta():
    from common.models import is_canary

    assert is_canary({"meta": {"domain": "canary"}}) is True
    assert is_canary({"meta": {"domain": "en.wikipedia.org"}}) is False
    assert is_canary({"meta": None}) is False
    assert is_canary({}) is False
