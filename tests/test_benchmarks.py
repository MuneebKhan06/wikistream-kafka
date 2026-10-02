from scripts.benchmarks import last_summary


def test_last_summary_takes_the_final_matching_line_without_the_prefix():
    log = "\n".join(
        [
            "2026-10-02 12:00:00,000 INFO [cleaner] cleaner started, transactional id cleaner-1",
            "2026-10-02 12:00:05,000 INFO [cleaner] stopped, 10 cleaned, 0 duplicates dropped",
            "2026-10-02 12:03:00,000 INFO [cleaner] stopped, 6044 cleaned, 0 duplicates dropped",
        ]
    )
    assert last_summary(log, "stopped,") == "stopped, 6044 cleaned, 0 duplicates dropped"


def test_last_summary_is_empty_when_nothing_matches():
    assert last_summary("no such line", "stopped,") == ""
