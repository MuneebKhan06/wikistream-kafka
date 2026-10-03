"""API tests against a fake database, so they run without PostgreSQL or Kafka."""

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from web import queries
from web.app import create_app
from web.cache import TTLCache

NOW = datetime(2026, 10, 3, 12, 0, 30, tzinfo=timezone.utc)


class FakeDatabase:
    def __init__(self, healthy=True, rows=None):
        self.healthy = healthy
        self.queries = []
        self.answers = rows or {}

    def ping(self):
        return self.healthy

    def rows(self, sql, params=None):
        self.queries.append(sql)
        for marker, answer in self.answers.items():
            if marker in sql:
                return answer
        return []

    def one(self, sql, params=None):
        rows = self.rows(sql, params)
        return rows[0] if rows else {}

    def close(self):
        pass


class FakeAdmin:
    def __init__(self, healthy=True):
        self.healthy = healthy

    def list_topics(self, timeout=None):
        if not self.healthy:
            raise RuntimeError("no brokers")
        return {}


def client(db=None, admin=None):
    return TestClient(
        create_app(db=db or FakeDatabase(), admin=admin or FakeAdmin(), pages=FakePageStore())
    )


def test_health_is_ok_when_both_stores_answer():
    response = client().get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": True, "kafka": True}


def test_health_reports_which_store_is_down():
    response = client(admin=FakeAdmin(healthy=False)).get("/api/health")
    assert response.status_code == 503
    assert response.json()["kafka"] is False
    assert response.json()["database"] is True


def test_minute_series_fills_quiet_minutes_and_drops_the_partial_one():
    rows = [
        {"minute": datetime(2026, 10, 3, 11, 58, tzinfo=timezone.utc), "edits": 7},
        {"minute": datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc), "edits": 3},
    ]
    series = queries.minute_series(rows, 3, NOW)
    assert [point["edits"] for point in series] == [0, 7, 0]
    assert series[-1]["minute"] == "2026-10-03T11:59:00+00:00"


def test_windows_are_clamped():
    assert queries.clamp_minutes(1) == queries.MIN_MINUTES
    assert queries.clamp_minutes(10**6) == queries.MAX_MINUTES


def test_overview_shapes_the_answer():
    db = FakeDatabase(
        rows={
            "window_edits": [{"edits": 200, "bot_edits": 50, "wikis": 4, "pages": 120}],
            "edits_stored": [{"edits_stored": 9000, "edit_wars": 2, "newest_stored_at": NOW,
                              "newest_event_time": NOW}],
            "60 seconds": [{"edits": 90}],
        }
    )
    result = queries.overview(db, 60, now=NOW)
    assert result["edits"] == 200
    assert result["bot_share"] == 0.25
    assert result["edits_per_second"] == 1.5
    assert result["edit_wars"] == 2
    assert len(result["per_minute"]) == 60


def test_overview_endpoint_rejects_out_of_range_windows():
    assert client().get("/api/overview?minutes=0").status_code == 422
    assert client().get("/api/overview?minutes=100000").status_code == 422


def test_cache_answers_repeat_questions_without_recomputing():
    clock = {"t": 0.0}
    cache = TTLCache(5, clock=lambda: clock["t"])
    calls = []
    compute = lambda: calls.append(1) or len(calls)  # noqa: E731
    assert cache.get("k", compute) == 1
    clock["t"] = 4.9
    assert cache.get("k", compute) == 1
    clock["t"] = 5.1
    assert cache.get("k", compute) == 2


def test_trending_attaches_a_sparkline_to_each_page():
    minute = datetime(2026, 10, 3, 11, 58, tzinfo=timezone.utc)
    db = FakeDatabase(
        rows={
            "SUM(edit_count)": [
                {"wiki": "enwiki", "title": "Paris", "edits": 12},
                {"wiki": "dewiki", "title": "Berlin", "edits": 5},
            ],
            "unnest": [{"wiki": "enwiki", "title": "Paris", "minute": minute, "edit_count": 12}],
            "MAX(minute)": [{"newest": minute}],
        }
    )
    result = queries.trending(db, 5, None, 10, now=NOW)
    paris, berlin = result["pages"]
    assert paris["edits"] == 12 and len(paris["per_minute"]) == 5
    assert paris["per_minute"][-2] == 12
    assert berlin["per_minute"] == []
    assert result["newest_minute"] == "2026-10-03T11:58:00+00:00"


def test_latest_edits_computes_size_change_and_link():
    db = FakeDatabase(
        rows={
            "ORDER BY event_time DESC": [
                {
                    "event_id": "e1", "wiki": "enwiki", "title": "C++ (language)", "type": "edit",
                    "namespace": 0, "username": "Alice", "bot": False, "minor": False,
                    "is_revert": True, "comment": "rv", "event_time": NOW,
                    "length_old": 100, "length_new": 80, "server_name": "en.wikipedia.org",
                    "ingested_at": NOW.replace(second=32),
                }
            ]
        }
    )
    result = queries.latest_edits(db, 10)
    edit = result["edits"][0]
    assert edit["size_change"] == -20
    assert edit["stored_after_ms"] == 2000
    assert edit["url"] == "https://en.wikipedia.org/wiki/C%2B%2B_(language)"
    assert result["next_before"] == edit["event_time"]


def test_page_url_needs_a_server():
    assert queries.page_url("", "Paris") is None


def test_alerts_report_how_long_each_war_lasted():
    start = datetime(2026, 10, 3, 11, 0, tzinfo=timezone.utc)
    db = FakeDatabase(
        rows={
            "FROM alerts": [
                {"alert_id": "a1", "wiki": "enwiki", "title": "Paris", "revert_count": 3,
                 "users": ["Alice", "Bob"], "window_start": start,
                 "window_end": start.replace(minute=12), "detected_at": NOW}
            ]
        }
    )
    alert = queries.alerts(db)["alerts"][0]
    assert alert["minutes"] == 12.0
    assert alert["users"] == ["Alice", "Bob"]


def test_feed_endpoint_validates_its_parameters():
    api = client()
    assert api.get("/api/edits?limit=0").status_code == 422
    assert api.get("/api/edits?limit=500").status_code == 422
    assert api.get("/api/edits?before=not-a-time").status_code == 422
    assert api.get("/api/edits?humans_only=true&reverts_only=true").status_code == 200
    assert api.get("/api/trending?limit=51").status_code == 422


# Kafka backed endpoints, with a fake page store and no broker.


class FakePageStore:
    def __init__(self, pages=()):
        self.pages = {f"{p.wiki}:{p.title}": p for p in pages}
        self.started = False

    def start(self):
        self.started = True

    def stop(self):
        pass

    def get(self, wiki, title):
        return self.pages.get(f"{wiki}:{title}")

    def search(self, query, wiki=None, limit=20):
        return [p for p in self.pages.values() if query.lower() in p.title.lower()][:limit]

    def stats(self):
        return {"pages": len(self.pages), "caught_up": True}


def make_page(title, wiki="enwiki"):
    from processors.page_latest import PageLatest

    return PageLatest(
        wiki=wiki, title=title, event_id="e", type="edit", user="Alice", bot=False,
        minor=False, is_revert=False, comment="c", event_time="2026-10-03T10:00:00+00:00",
        rev_id=7, length=100,
    )


def test_page_lookup_and_search():
    store = FakePageStore([make_page("Paris"), make_page("Paris Hilton"), make_page("Rome")])
    api = TestClient(create_app(db=FakeDatabase(), admin=FakeAdmin(), pages=store))
    assert api.get("/api/pages/enwiki/Paris").json()["rev_id"] == 7
    found = api.get("/api/pages?q=paris").json()
    assert {p["title"] for p in found["pages"]} == {"Paris", "Paris Hilton"}
    assert found["snapshot"]["pages"] == 3


def test_a_title_with_a_slash_is_one_page():
    store = FakePageStore([make_page("AC/DC")])
    api = TestClient(create_app(db=FakeDatabase(), admin=FakeAdmin(), pages=store))
    assert api.get("/api/pages/enwiki/AC/DC").json()["title"] == "AC/DC"


def test_a_missing_page_is_a_404():
    api = TestClient(create_app(db=FakeDatabase(), admin=FakeAdmin(), pages=FakePageStore()))
    assert api.get("/api/pages/enwiki/Nowhere").status_code == 404


def test_the_page_store_starts_with_the_app():
    store = FakePageStore()
    with TestClient(create_app(db=FakeDatabase(), admin=FakeAdmin(), pages=store)):
        assert store.started


def test_page_store_applies_records_and_tombstones():
    from web.pages import PageStore

    store = PageStore(topic="unused")
    store.apply(b"enwiki:Paris", make_page("Paris").to_json())
    store.apply(b"enwiki:Rome", make_page("Rome").to_json())
    store.apply(b"enwiki:Rome", None)
    assert store.get("enwiki", "Paris").rev_id == 7
    assert store.get("enwiki", "Rome") is None
    stats = store.stats()
    assert (stats["pages"], stats["records_read"], stats["tombstones"]) == (1, 3, 1)


def test_benchmarks_endpoint_serves_the_recorded_results():
    api = TestClient(create_app(db=FakeDatabase(), admin=FakeAdmin(), pages=FakePageStore()))
    body = api.get("/api/benchmarks").json()
    assert {"failure_tests", "performance", "rebalance"} <= set(body)
