import json

from ingestor.checkpoint import Checkpoint


def test_missing_file_starts_empty(tmp_path):
    cp = Checkpoint.load(tmp_path / "ingestor.json")
    assert cp.last_event_id is None
    assert cp.resume_header() == {}


def test_save_then_load_round_trip(tmp_path):
    path = tmp_path / "state" / "ingestor.json"
    cp = Checkpoint(path=path)
    cp.advance('[{"topic":"codfw.mediawiki.recentchange","offset":2082563943}]')
    cp.save()

    reloaded = Checkpoint.load(path)
    assert reloaded.last_event_id == cp.last_event_id
    assert reloaded.events_confirmed == 1
    assert reloaded.resume_header()["Last-Event-ID"] == cp.last_event_id


def test_advance_counts_events_without_id(tmp_path):
    cp = Checkpoint(path=tmp_path / "c.json", last_event_id="abc")
    cp.advance(None)
    assert cp.last_event_id == "abc"
    assert cp.events_confirmed == 1


def test_corrupt_file_does_not_crash(tmp_path):
    path = tmp_path / "ingestor.json"
    path.write_text("{not json", encoding="utf-8")
    assert Checkpoint.load(path).last_event_id is None


def test_save_leaves_no_temp_files_behind(tmp_path):
    path = tmp_path / "ingestor.json"
    cp = Checkpoint(path=path, last_event_id="id-1")
    cp.save()
    cp.advance("id-2")
    cp.save()

    assert json.loads(path.read_text())["last_event_id"] == "id-2"
    assert [p.name for p in tmp_path.iterdir()] == ["ingestor.json"]


def test_advance_can_cover_several_events(tmp_path):
    cp = Checkpoint(path=tmp_path / "c.json")
    cp.advance("id-3", count=3)
    assert cp.last_event_id == "id-3"
    assert cp.events_confirmed == 3
