from ingestor.main import DeliveryTracker, Ingestor


class FakeError:
    def __str__(self):
        return "NOT_ENOUGH_REPLICAS"


def make_ingestor(tmp_path):
    return Ingestor(checkpoint_path=tmp_path / "ingestor.json")


def test_failed_delivery_stops_the_ingestor(tmp_path):
    ingestor = make_ingestor(tmp_path)
    ingestor.tracker.register(1, "a")
    ingestor._on_delivery(FakeError(), None, 1, "a")
    assert ingestor.running is False
    assert ingestor.failure is not None


def test_checkpoint_stays_behind_a_failed_delivery(tmp_path):
    """Later successes must not carry the checkpoint past the lost event."""
    ingestor = make_ingestor(tmp_path)
    ingestor.tracker.register(1, "before")
    ingestor.tracker.register(2, "failed")
    ingestor.tracker.register(3, "after")

    ingestor._on_delivery(None, None, 1, "before")
    ingestor._on_delivery(FakeError(), None, 2, "failed")
    ingestor._on_delivery(None, None, 3, "after")

    assert ingestor.checkpoint.last_event_id == "before"
    assert ingestor.tracker.in_flight == 2


def test_only_the_first_failure_is_kept(tmp_path):
    ingestor = make_ingestor(tmp_path)
    first, second = FakeError(), FakeError()
    ingestor.tracker.register(1, "a")
    ingestor.tracker.register(2, "b")
    ingestor._on_delivery(first, None, 1, "a")
    ingestor._on_delivery(second, None, 2, "b")
    assert ingestor.failure is first


def test_in_order_delivery_releases_each_id():
    tracker = DeliveryTracker()
    for seq, sid in [(1, "a"), (2, "b"), (3, "c")]:
        tracker.register(seq, sid)
    assert tracker.confirm(1) == ("a", 1)
    assert tracker.confirm(2) == ("b", 1)
    assert tracker.confirm(3) == ("c", 1)
    assert tracker.in_flight == 0


def test_out_of_order_delivery_holds_back_checkpoint():
    tracker = DeliveryTracker()
    for seq, sid in [(1, "a"), (2, "b"), (3, "c")]:
        tracker.register(seq, sid)

    assert tracker.confirm(3) == (None, 0)
    assert tracker.confirm(2) == (None, 0)
    assert tracker.in_flight == 3
    # Only once the oldest is confirmed does the whole prefix release,
    # counting every message it covers.
    assert tracker.confirm(1) == ("c", 3)
    assert tracker.in_flight == 0


def test_events_without_stream_id_do_not_lose_the_prefix():
    tracker = DeliveryTracker()
    tracker.register(1, "a")
    tracker.register(2, None)
    assert tracker.confirm(1) == ("a", 1)
    assert tracker.confirm(2) == (None, 1)
    assert tracker.in_flight == 0
