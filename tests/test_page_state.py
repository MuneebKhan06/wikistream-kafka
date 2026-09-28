import json

from common.models import CleanEvent, clean_event, parse_raw
from processors.page_latest import PageLatest, is_deletion, page_update


def event(**overrides):
    raw = {
        "meta": {"id": "e-1", "dt": "2026-09-28T10:00:00Z"},
        "type": "edit",
        "namespace": 0,
        "title": "Roma",
        "comment": "fix typo",
        "user": "Alice",
        "bot": False,
        "minor": True,
        "wiki": "itwiki",
        "server_name": "it.wikipedia.org",
        "revision": {"old": 1, "new": 2},
        "length": {"old": 10, "new": 12},
    }
    raw.update(overrides)
    return clean_event(parse_raw(json.dumps(raw).encode()))


def test_an_edit_becomes_the_latest_record_for_its_page():
    key, record = page_update(event())
    assert key == "itwiki:Roma"
    assert record.rev_id == 2
    assert record.length == 12
    assert record.user == "Alice"
    assert record.page_key == key


def test_a_new_page_is_a_content_change_too():
    key, record = page_update(event(type="new"))
    assert record.type == "new"


def test_a_deletion_becomes_a_tombstone():
    deletion = event(type="log", log_type="delete", log_action="delete")
    assert is_deletion(deletion)
    assert page_update(deletion) == ("itwiki:Roma", None)


def test_other_log_events_leave_the_page_alone():
    for log_type, log_action in [
        ("delete", "restore"),
        ("patrol", "patrol"),
        ("upload", "upload"),
        ("thanks", "thank"),
    ]:
        assert page_update(event(type="log", log_type=log_type, log_action=log_action)) is None


def test_category_changes_leave_the_page_alone():
    assert page_update(event(type="categorize")) is None


def test_reverts_are_flagged_on_the_latest_record():
    _, record = page_update(event(comment="Reverted edits by X"))
    assert record.is_revert is True


def test_record_round_trips_through_json():
    _, record = page_update(event(title="Zollingerdächer"))
    assert PageLatest.from_json(record.to_json()) == record


def test_log_fields_are_captured_from_the_raw_event():
    deletion = event(type="log", log_type="delete", log_action="delete")
    assert (deletion.log_type, deletion.log_action) == ("delete", "delete")
    assert event().log_type is None


def test_records_written_before_log_fields_existed_still_decode():
    old = json.dumps(
        {
            "event_id": "e-1",
            "wiki": "itwiki",
            "title": "Roma",
            "type": "edit",
            "namespace": 0,
            "user": "Alice",
            "bot": False,
            "minor": False,
            "comment": "",
            "event_time": "2026-09-25T10:00:00+00:00",
            "rev_old": 1,
            "rev_new": 2,
            "length_old": 1,
            "length_new": 2,
            "server_name": "it.wikipedia.org",
        }
    ).encode()
    decoded = CleanEvent.from_json(old)
    assert decoded.log_type is None
    assert page_update(decoded)[1].rev_id == 2
