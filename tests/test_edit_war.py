import json

from common.models import clean_event, parse_raw
from processors.edit_war_detector import (
    MINUTE_MS,
    EditWarAlert,
    EditWarDetector,
    alert_id_for,
)

BASE = 1_790_000_000_000


def event(event_id, user, comment="Reverted edits by X", title="Roma", bot=False):
    raw = {
        "meta": {"id": event_id, "dt": "2026-09-27T10:00:00Z"},
        "type": "edit",
        "namespace": 0,
        "title": title,
        "comment": comment,
        "user": user,
        "bot": bot,
        "wiki": "enwiki",
        "server_name": "en.wikipedia.org",
    }
    return clean_event(parse_raw(json.dumps(raw).encode()))


def feed(detector, items):
    """items: (event_id, user, minute, extra kwargs) -> list of alerts."""
    alerts = []
    for event_id, user, minute, *extra in items:
        kwargs = extra[0] if extra else {}
        alert = detector.observe(event(event_id, user, **kwargs), BASE + minute * MINUTE_MS)
        if alert:
            alerts.append(alert)
    return alerts


def test_three_reverts_by_two_people_is_a_war():
    alerts = feed(EditWarDetector(), [("a", "Alice", 0), ("b", "Bob", 2), ("c", "Alice", 4)])
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.revert_count == 3
    assert alert.users == ("Alice", "Bob")
    assert alert.event_ids == ("a", "b", "c")


def test_one_person_reverting_alone_is_not_a_war():
    alerts = feed(EditWarDetector(), [("a", "Alice", 0), ("b", "Alice", 1), ("c", "Alice", 2)])
    assert alerts == []


def test_reverts_spread_beyond_the_window_do_not_count():
    detector = EditWarDetector(window_ms=30 * MINUTE_MS)
    alerts = feed(detector, [("a", "Alice", 0), ("b", "Bob", 20), ("c", "Alice", 45)])
    assert alerts == []


def test_normal_edits_are_ignored():
    items = [(f"e{i}", f"User{i}", i, {"comment": "copyedit"}) for i in range(10)]
    assert feed(EditWarDetector(), items) == []


def test_bot_reverts_are_skipped_by_default():
    items = [
        ("a", "Alice", 0),
        ("b", "CleanupBot", 1, {"bot": True}),
        ("c", "CleanupBot", 2, {"bot": True}),
    ]
    assert feed(EditWarDetector(), items) == []
    assert len(feed(EditWarDetector(ignore_bots=False), items)) == 1


def test_an_ongoing_war_alerts_once():
    items = [(f"r{i}", "Alice" if i % 2 else "Bob", i) for i in range(10)]
    assert len(feed(EditWarDetector(), items)) == 1


def test_a_new_war_after_a_quiet_period_alerts_again():
    detector = EditWarDetector(window_ms=30 * MINUTE_MS)
    first = [("a", "Alice", 0), ("b", "Bob", 1), ("c", "Alice", 2)]
    second = [("d", "Carol", 120), ("e", "Dan", 121), ("f", "Carol", 122)]
    alerts = feed(detector, first + second)
    assert len(alerts) == 2
    assert alerts[0].alert_id != alerts[1].alert_id


def test_pages_are_tracked_separately():
    items = [
        ("a", "Alice", 0, {"title": "Roma"}),
        ("b", "Bob", 1, {"title": "Paris"}),
        ("c", "Alice", 2, {"title": "Roma"}),
    ]
    assert feed(EditWarDetector(), items) == []


def test_replaying_the_same_events_gives_the_same_alert_id():
    items = [("a", "Alice", 0), ("b", "Bob", 2), ("c", "Alice", 4)]
    first = feed(EditWarDetector(), items)[0]
    second = feed(EditWarDetector(), items)[0]
    assert first.alert_id == second.alert_id == alert_id_for("enwiki", "Roma", "a")


def test_alert_round_trips_through_json():
    alert = feed(EditWarDetector(), [("a", "Alice", 0), ("b", "Bob", 2), ("c", "Alice", 4)])[0]
    assert EditWarAlert.from_json(alert.to_json()) == alert
    assert alert.page_key == "enwiki:Roma"


def test_quiet_pages_are_expired_from_memory():
    detector = EditWarDetector(window_ms=30 * MINUTE_MS)
    feed(detector, [("a", "Alice", 0, {"title": "Old"})])
    feed(detector, [("b", "Bob", 90, {"comment": "copyedit", "title": "New"})])
    assert detector.expire() == 1
    assert "enwiki:Old" not in detector.pages


def test_multilingual_reverts_feed_the_detector():
    items = [
        ("a", "Alice", 0, {"comment": "Änderung 1 von X rückgängig gemacht"}),
        ("b", "Bob", 1, {"comment": "Revertida una edición de X"}),
        ("c", "Alice", 2, {"comment": "回退X做出的1次編輯"}),
    ]
    assert len(feed(EditWarDetector(), items)) == 1
