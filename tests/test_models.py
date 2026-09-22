import json

import pytest

from common.models import CleanEvent, ParseError, clean_event, parse_raw


def make_raw(**overrides):
    raw = {
        "meta": {
            "id": "df6a0755-f8fe-48cb-9bce-4dc70b644289",
            "dt": "2026-09-22T17:15:47.273Z",
            "domain": "it.wikipedia.org",
        },
        "id": 369694821,
        "type": "edit",
        "namespace": 0,
        "title": "Roma",
        "comment": "fix typo",
        "timestamp": 1790097342,
        "user": "Alice",
        "bot": False,
        "minor": True,
        "length": {"old": 12499, "new": 12523},
        "revision": {"old": 137979392, "new": 152573130},
        "server_name": "it.wikipedia.org",
        "wiki": "itwiki",
    }
    raw.update(overrides)
    return raw


def test_clean_event_maps_fields():
    event = clean_event(make_raw())
    assert event.event_id == "df6a0755-f8fe-48cb-9bce-4dc70b644289"
    assert event.page_key == "itwiki:Roma"
    assert event.event_time == "2026-09-22T17:15:47.273000+00:00"
    assert event.size_delta == 24
    assert event.minor is True


def test_title_whitespace_is_normalised():
    assert clean_event(make_raw(title="  New   York ")).title == "New York"


def test_event_without_revision_or_length():
    raw = make_raw(type="categorize")
    del raw["revision"], raw["length"]
    event = clean_event(raw)
    assert event.rev_new is None
    assert event.size_delta is None


def test_falls_back_to_unix_timestamp():
    raw = make_raw()
    raw["meta"] = {"id": "abc"}
    assert clean_event(raw).event_time.startswith("2026-09-22T")


@pytest.mark.parametrize(
    "overrides",
    [
        {"meta": {}},
        {"type": "unknown"},
        {"wiki": ""},
        {"title": "   "},
        {"namespace": "0"},
        {"revision": {"old": "1", "new": 2}},
    ],
)
def test_invalid_events_raise(overrides):
    with pytest.raises(ParseError):
        clean_event(make_raw(**overrides))


@pytest.mark.parametrize("payload", [b"not json", b"[1, 2]", b"\xff\xfe"])
def test_parse_raw_rejects_bad_payloads(payload):
    with pytest.raises(ParseError):
        parse_raw(payload)


@pytest.mark.parametrize(
    "comment, expected",
    [
        ("Reverted edits by X to last version by Y", True),
        ("Undid revision 123 by X", True),
        ("rv vandalism", True),
        ("added references", False),
        ("improved the prevention section", False),
    ],
)
def test_revert_detection(comment, expected):
    assert clean_event(make_raw(comment=comment)).is_revert is expected


def test_json_round_trip():
    event = clean_event(make_raw(title="Zollingerdächer"))
    assert CleanEvent.from_json(event.to_json()) == event
    assert "Zollingerdächer" in json.loads(event.to_json())["title"]
