from ingestor.main import DeliveryTracker


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
