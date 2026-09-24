from processors.dedup import DedupCache

BASE = 1_790_000_000_000


def test_first_sighting_is_new_and_repeat_is_not():
    cache = DedupCache()
    assert cache.is_new("a", BASE) is True
    assert cache.is_new("a", BASE) is False
    assert cache.duplicate_count == 1


def test_duplicate_is_caught_across_buckets():
    cache = DedupCache(window_ms=3600_000, buckets=12)
    cache.is_new("a", BASE)
    # Same id 50 minutes later, still inside the one hour window.
    assert cache.is_new("a", BASE + 50 * 60_000) is False


def test_id_expires_once_it_leaves_the_window():
    cache = DedupCache(window_ms=3600_000, buckets=12)
    cache.is_new("a", BASE)
    assert cache.is_new("a", BASE + 2 * 3600_000) is True


def test_old_buckets_are_dropped():
    cache = DedupCache(window_ms=3600_000, buckets=12)
    for minute in range(0, 180, 5):
        cache.is_new(f"id-{minute}", BASE + minute * 60_000)
    assert len(cache._buckets) <= 13
    assert cache.size < 36


def test_size_cap_drops_oldest_first():
    cache = DedupCache(window_ms=3600_000, buckets=12, max_ids=10)
    for i in range(40):
        cache.is_new(f"id-{i}", BASE + i * 60_000)
    assert cache.size <= 10
    assert cache.evicted_early > 0


def test_clear_forgets_everything():
    cache = DedupCache()
    cache.is_new("a", BASE)
    cache.clear()
    assert cache.size == 0
    assert cache.is_new("a", BASE) is True


def test_stats_report_counters():
    cache = DedupCache()
    cache.is_new("a", BASE)
    cache.is_new("a", BASE)
    stats = cache.stats()
    assert stats["seen"] == 2
    assert stats["duplicates"] == 1
    assert stats["ids"] == 1
