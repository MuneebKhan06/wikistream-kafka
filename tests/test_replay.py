import json
import time

from scripts.replay import load, pace, parse_args


def write_sample(tmp_path, count=5, start_ts=1790000000):
    path = tmp_path / "sample.jsonl"
    with path.open("w", encoding="utf-8") as handle:
        for i in range(count):
            handle.write(
                json.dumps(
                    {
                        "meta": {"id": f"e-{i}"},
                        "timestamp": start_ts + i,
                        "type": "edit",
                        "wiki": "enwiki",
                        "title": "Roma",
                        "namespace": 0,
                    }
                )
                + "\n"
            )
    return path


def test_load_reads_ids_and_timestamps(tmp_path):
    records = load(write_sample(tmp_path, 3))
    assert [r[0] for r in records] == ["e-0", "e-1", "e-2"]
    assert records[1][2] - records[0][2] == 1


def test_load_skips_bad_lines(tmp_path):
    path = write_sample(tmp_path, 2)
    with path.open("a", encoding="utf-8") as handle:
        handle.write("{broken\n\n")
    assert len(load(path)) == 2


def test_pace_does_not_sleep_without_speed_or_rate(tmp_path):
    records = load(write_sample(tmp_path, 3))
    args = parse_args([])
    started = time.monotonic()
    pace(records, 2, started, args)
    assert time.monotonic() - started < 0.05


def test_pace_waits_for_a_fixed_rate(tmp_path):
    records = load(write_sample(tmp_path, 3))
    args = parse_args(["--rate", "20"])
    started = time.monotonic()
    pace(records, 2, started, args)
    assert time.monotonic() - started >= 0.09


def test_speed_divides_the_original_gap(tmp_path):
    records = load(write_sample(tmp_path, 3))
    args = parse_args(["--speed", "20"])
    started = time.monotonic()
    pace(records, 2, started, args)
    elapsed = time.monotonic() - started
    assert 0.08 <= elapsed < 0.3


def test_defaults(tmp_path):
    args = parse_args([])
    assert args.loops == 1
    assert args.topic == "wiki.raw"
    assert args.acks is None


def test_acks_override_disables_idempotence():
    from scripts.replay import producer_overrides

    assert producer_overrides(None) == {}
    assert producer_overrides("all") == {}
    assert producer_overrides("1") == {"acks": "1", "enable.idempotence": False}


def test_default_sample_is_found_from_any_directory(tmp_path, monkeypatch):
    from scripts.replay import DEFAULT_SAMPLE

    monkeypatch.chdir(tmp_path)
    assert DEFAULT_SAMPLE.is_absolute()
    assert DEFAULT_SAMPLE.exists()


def test_rate_keeps_throttling_on_later_passes(tmp_path):
    """The second pass continues the schedule rather than starting over."""
    records = load(write_sample(tmp_path, 3))
    args = parse_args(["--rate", "20", "--loops", "2"])
    started = time.monotonic()
    # First record of the second pass is the 4th send overall: 3 / 20 s in.
    pace(records, 0, started, args, loop=1)
    assert time.monotonic() - started >= 0.14


def test_speed_offsets_later_passes_by_the_file_span(tmp_path):
    records = load(write_sample(tmp_path, 3))
    args = parse_args(["--speed", "20", "--loops", "2"])
    started = time.monotonic()
    # The file spans 2 s, plus 1 s between passes, so pass 2 starts 3 s in.
    pace(records, 0, started, args, loop=1)
    assert time.monotonic() - started >= 0.14


def test_unique_copy_changes_the_id_in_key_and_payload():
    from scripts.replay import unique_copy

    payload = json.dumps({"meta": {"id": "abc-123"}, "title": "abc-123 in a title"}).encode()
    new_id, new_payload = unique_copy("abc-123", payload, 4)
    assert new_id == "abc-123-4"
    decoded = json.loads(new_payload)
    assert decoded["meta"]["id"] == "abc-123-4"
    # Only the first occurrence, the id itself, is rewritten.
    assert decoded["title"] == "abc-123 in a title"


def test_unique_ids_is_off_by_default():
    assert parse_args([]).unique_ids is False
    assert parse_args(["--unique-ids"]).unique_ids is True
