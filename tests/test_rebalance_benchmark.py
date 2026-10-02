from scripts.rebalance_benchmark import analyse, longest_gap, owner_at


def test_longest_gap_includes_the_window_edges():
    assert longest_gap([1.0, 2.0, 3.0], 0.0, 10.0) == 7.0
    assert longest_gap([], 0.0, 5.0) == 5.0
    assert round(longest_gap([0.1 * i for i in range(100)], 0.0, 9.9), 3) == 0.1


def test_owner_is_whoever_received_the_partition_last():
    per_worker = {"a": {0: [1.0, 2.0]}, "b": {0: [3.0, 4.0]}}
    assert owner_at(per_worker, 0, 2.5) == "a"
    assert owner_at(per_worker, 0, 5.0) == "b"
    assert owner_at(per_worker, 0, 0.5) is None


def test_a_moved_partition_shows_its_pause_and_an_unmoved_one_does_not():
    steady = [0.1 * i for i in range(200)]             # 0 to 20 s, never stops
    before = [0.1 * i for i in range(100)]             # consumer a until 10 s
    after = [13.0 + 0.1 * i for i in range(70)]        # consumer b from 13 s
    per_worker = {
        "a": {0: steady, 1: before, **{p: steady for p in range(2, 6)}},
        "b": {1: after},
    }
    result = analyse(per_worker, {"join": 10.0})["join"]
    assert result["partitions_that_changed_owner"] == 1
    assert result["partitions_that_stopped"] == 1
    assert 2.9 <= result["pause_per_partition_sec"]["1"] <= 3.2
    assert result["pause_per_partition_sec"]["0"] < 0.2
