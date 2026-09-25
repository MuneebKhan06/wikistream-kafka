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
