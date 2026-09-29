from collections import Counter

from scripts.pipeline_report import fmt_ms, hottest_minutes, parse_args


def scan(per_minute, keys):
    return {"partitions": 3, "per_minute": per_minute, "keys": keys, "totals": Counter()}


def test_minutes_are_ranked_by_the_largest_partition_share():
    data = scan(
        {
            1: Counter({0: 50, 1: 50, 2: 100}),  # p2 has 50%
            2: Counter({0: 280, 1: 10, 2: 10}),  # p0 has 93%
        },
        {
            (1, 2): Counter({b"enwiki:A": 60, b"enwiki:B": 40}),
            (2, 0): Counter({b"enwiki:Breaking": 250, b"enwiki:C": 30}),
        },
    )
    rows = hottest_minutes(data, min_events=100, limit=5)
    assert [r["minute"] for r in rows] == [2, 1]
    assert rows[0]["partition"] == 0
    assert round(rows[0]["share"], 3) == round(280 / 300, 3)
    assert rows[0]["key"] == "enwiki:Breaking"


def test_quiet_minutes_are_left_out():
    data = scan({1: Counter({0: 5})}, {(1, 0): Counter({b"k": 5})})
    assert hottest_minutes(data, min_events=100, limit=5) == []


def test_durations_are_readable():
    assert fmt_ms(None) == "n/a"
    assert fmt_ms(250) == "250 ms"
    assert fmt_ms(2500) == "2.50 s"
    assert fmt_ms(180_000) == "3.0 min"


def test_both_reports_run_by_default():
    args = parse_args([])
    assert args.latency and args.partitions
    only = parse_args(["--latency"])
    assert only.latency and not only.partitions
