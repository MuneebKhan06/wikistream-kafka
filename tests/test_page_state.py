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


# Ordering: arrival order in wiki.clean is not event order.

import logging  # noqa: E402

from processors.page_latest import event_ms  # noqa: E402
from processors.page_state import PageState, is_stale, remember, time_of_record  # noqa: E402


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key=None, value=None, timestamp=0):
        self.sent.append((key.decode(), value, timestamp))


class FakeMeter:
    def mark(self, *_):
        pass

    def mark_error(self, *_):
        pass


class FakeMessage:
    def __init__(self, event, partition=0):
        self._value = event.to_json()
        self._partition = partition

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return 0


def processor():
    p = PageState.__new__(PageState)
    p.log = logging.getLogger("test")
    p.producer = FakeProducer()
    p.meter = FakeMeter()
    p.applied = {}
    p.updates = p.tombstones = p.stale = p.ignored = 0
    return p


def at(second, **overrides):
    return event(meta={"id": f"e-{second}-{overrides.get('type', 'edit')}",
                       "dt": f"2026-09-28T10:00:{second:02d}Z"}, **overrides)


def test_an_older_edit_arriving_late_does_not_replace_the_newer_one():
    p = processor()
    p.handle(FakeMessage(at(20, revision={"old": 1, "new": 3})))
    p.handle(FakeMessage(at(10, revision={"old": 1, "new": 2})))
    assert len(p.producer.sent) == 1
    assert p.stale == 1


def test_an_edit_arriving_after_its_pages_deletion_does_not_bring_it_back():
    p = processor()
    p.handle(FakeMessage(at(30, type="log", log_type="delete", log_action="delete")))
    p.handle(FakeMessage(at(10)))
    assert [value for _, value, _ in p.producer.sent] == [None]
    assert p.stale == 1


def test_a_page_recreated_after_deletion_is_written():
    p = processor()
    p.handle(FakeMessage(at(10, type="log", log_type="delete", log_action="delete")))
    p.handle(FakeMessage(at(40, type="new")))
    assert p.producer.sent[-1][1] is not None
    assert p.tombstones == 1 and p.updates == 1


def test_records_are_stamped_with_their_event_time():
    p = processor()
    p.handle(FakeMessage(at(20)))
    assert p.producer.sent[0][2] == event_ms("2026-09-28T10:00:20+00:00")


def test_equal_times_still_apply():
    applied = {}
    remember(applied, "k", 100)
    assert is_stale(applied, "k", 100) is False
    assert is_stale(applied, "k", 99) is True


def test_remembered_times_only_move_forward():
    applied = {}
    remember(applied, "k", 200)
    remember(applied, "k", 100)
    assert applied["k"] == 200


def test_a_tombstone_is_timed_by_its_record_timestamp():
    assert time_of_record(None, 12345) == 12345
    _, record = page_update(at(20))
    assert time_of_record(record.to_json(), 1) == event_ms("2026-09-28T10:00:20+00:00")
