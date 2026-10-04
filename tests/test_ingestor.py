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


class QueueFullProducer:
    """Raises BufferError for the first `full_for` produce calls."""

    def __init__(self, full_for):
        self.full_for = full_for
        self.produced = []
        self.polls = 0

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self.full_for > 0:
            self.full_for -= 1
            raise BufferError("Local: Queue full")
        self.produced.append(value)

    def poll(self, timeout=0):
        self.polls += 1
        return 0


def test_a_briefly_full_queue_is_waited_out(tmp_path):
    ingestor = make_ingestor(tmp_path)
    ingestor.producer = QueueFullProducer(full_for=2)
    assert ingestor._send("wiki.raw", "k", "v", "id-1") is True
    assert ingestor.producer.produced == [b"v"]
    assert ingestor.producer.polls == 2
    assert ingestor.tracker.in_flight == 1
    assert ingestor.running is True


def test_a_queue_that_stays_full_stops_the_ingestor_cleanly(tmp_path, monkeypatch):
    import ingestor.main as main

    monkeypatch.setattr(main, "QUEUE_FULL_GIVE_UP_SEC", 0.0)
    ingestor = make_ingestor(tmp_path)
    ingestor.producer = QueueFullProducer(full_for=10**6)
    assert ingestor._send("wiki.raw", "k", "v", "id-1") is False
    assert ingestor.running is False
    assert "queue full" in ingestor.failure
    # Nothing was sent, so nothing may hold the checkpoint back.
    assert ingestor.tracker.in_flight == 0
    assert ingestor.seq == 0
