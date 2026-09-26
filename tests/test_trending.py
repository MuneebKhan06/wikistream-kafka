from processors.windows import MINUTE_MS, MinuteWindows, minute_start

BASE = minute_start(1_790_000_000_000)


def add(windows, offset, title="Roma", minute=0, partition=0, wiki="enwiki"):
    return windows.add(partition, offset, wiki, title, BASE + minute * MINUTE_MS)


def test_edits_to_one_page_in_one_minute_are_counted_together():
    windows = MinuteWindows()
    for offset in range(3):
        add(windows, offset)
    assert windows.open_windows() == 1
    assert windows.counted == 3


def test_same_page_in_different_minutes_are_separate_windows():
    windows = MinuteWindows()
    add(windows, 0, minute=0)
    add(windows, 1, minute=1)
    assert windows.open_windows() == 2


def test_window_closes_once_the_stream_moves_past_it():
    windows = MinuteWindows(lateness_ms=MINUTE_MS)
    add(windows, 0, minute=0)
    assert windows.pop_closed() == []

    add(windows, 1, minute=3, title="Other")
    closed = windows.pop_closed()
    assert [(c.title, c.count) for c in closed] == [("Roma", 1)]


def test_closed_window_row_matches_the_table_columns():
    windows = MinuteWindows(lateness_ms=MINUTE_MS)
    add(windows, 0, minute=0)
    add(windows, 1, minute=5, title="Other")
    row = windows.pop_closed()[0].as_row()
    assert row[0] == "enwiki"
    assert row[1] == "Roma"
    assert row[2].endswith("+00:00")
    assert row[3] == 1


def test_commit_offset_is_the_oldest_offset_an_open_window_needs():
    windows = MinuteWindows(lateness_ms=MINUTE_MS)
    add(windows, 100, minute=0, title="Old")
    add(windows, 101, minute=5, title="New")
    # The older window still needs offset 100, so committing 102 would lose it.
    assert windows.commit_offsets() == {0: 100}

    windows.pop_closed()
    assert windows.commit_offsets() == {0: 101}


def test_commit_offset_moves_past_everything_once_all_windows_close():
    windows = MinuteWindows(lateness_ms=MINUTE_MS)
    add(windows, 7, minute=0)
    windows.add(0, 8, "enwiki", "Other", BASE + 10 * MINUTE_MS)
    windows.pop_closed()
    windows.state(0).windows.clear()
    assert windows.commit_offsets() == {0: 9}


def test_partitions_keep_their_own_offsets():
    windows = MinuteWindows()
    add(windows, 5, partition=0)
    add(windows, 40, partition=1)
    assert windows.commit_offsets() == {0: 5, 1: 40}


def test_late_event_for_a_flushed_minute_is_rejected():
    windows = MinuteWindows(lateness_ms=MINUTE_MS)
    add(windows, 0, minute=0)
    add(windows, 1, minute=5, title="Other")
    windows.pop_closed()

    assert add(windows, 2, minute=0) is False
    assert windows.late_events == 1


def test_replay_of_an_open_window_recomputes_the_full_count():
    """A restart re-reads from the commit offset, so the count is rebuilt."""
    first = MinuteWindows(lateness_ms=MINUTE_MS)
    for offset in range(4):
        add(first, offset)
    commit = first.commit_offsets()[0]

    second = MinuteWindows(lateness_ms=MINUTE_MS)
    for offset in range(commit, 4):
        add(second, offset)
    second.add(0, 4, "enwiki", "Other", BASE + 5 * MINUTE_MS)

    assert second.pop_closed()[0].count == 4


def test_revoked_partition_state_is_dropped():
    windows = MinuteWindows()
    add(windows, 1, partition=0)
    add(windows, 2, partition=1)
    windows.revoke([0])
    assert 0 not in windows.partitions
    assert windows.open_windows() == 1


def test_window_cap_forces_out_the_oldest_minute():
    windows = MinuteWindows(max_windows=5)
    for i in range(8):
        windows.add(0, i, "enwiki", f"Page-{i}", BASE + (i // 4) * MINUTE_MS)
    assert windows.open_windows() <= 5


def test_stats_report_progress():
    windows = MinuteWindows()
    add(windows, 0)
    stats = windows.stats()
    assert stats["counted"] == 1
    assert stats["open_windows"] == 1
    assert stats["watermark_ms"] == BASE
