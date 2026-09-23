"""Guards the recorded sample: every line must still parse cleanly."""

import json
from pathlib import Path

import pytest

from common.models import clean_event, parse_raw

SAMPLE = Path(__file__).resolve().parents[1] / "samples" / "recentchange_sample.jsonl"


@pytest.fixture(scope="module")
def events():
    if not SAMPLE.exists():
        pytest.skip("sample file not recorded")
    with SAMPLE.open(encoding="utf-8") as handle:
        return [clean_event(parse_raw(line)) for line in handle if line.strip()]


def test_sample_is_not_tiny(events):
    assert len(events) >= 500


def test_every_event_has_an_id_and_page_key(events):
    assert all(e.event_id and e.page_key for e in events)


def test_sample_covers_several_wikis_and_types(events):
    assert len({e.wiki for e in events}) >= 10
    assert len({e.type for e in events}) >= 2


def test_event_ids_are_unique(events):
    ids = [e.event_id for e in events]
    assert len(set(ids)) == len(ids)


def test_events_round_trip_through_json(events):
    sampled = events[:50]
    assert all(json.loads(e.to_json())["event_id"] == e.event_id for e in sampled)
