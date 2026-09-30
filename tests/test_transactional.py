"""The shared loop is tested with fake clients: no broker is needed."""

from confluent_kafka import KafkaError, KafkaException

from processors.transactional import TransactionalProcessor


class FakeError:
    def __init__(self, code=None, abortable=False, fatal=False):
        self._code = code
        self._abortable = abortable
        self._fatal = fatal

    def code(self):
        return self._code

    def fatal(self):
        return self._fatal

    def str(self):
        return f"error {self._code}"

    def txn_requires_abort(self):
        return self._abortable


class FakeMessage:
    def __init__(self, value, error=None):
        self._value = value
        self._error = error

    def value(self):
        return self._value

    def error(self):
        return self._error


class FakeProducer:
    def __init__(self, fail_commit_with=None):
        self.calls = []
        self.fail_commit_with = fail_commit_with

    def begin_transaction(self):
        self.calls.append("begin")

    def send_offsets_to_transaction(self, positions, metadata):
        self.calls.append("offsets")

    def commit_transaction(self):
        if self.fail_commit_with is not None:
            raise KafkaException(self.fail_commit_with)
        self.calls.append("commit")

    def abort_transaction(self):
        self.calls.append("abort")


class FakeConsumer:
    def __init__(self, messages=()):
        self.messages = list(messages)
        self.calls = []

    def poll(self, timeout):
        return self.messages.pop(0) if self.messages else None

    def position(self, assignment):
        return []

    def assignment(self):
        return []

    def consumer_group_metadata(self):
        return object()

    def committed(self, partitions, timeout=None):
        # No commits yet, so the retention check has nothing to report.
        from confluent_kafka import TopicPartition

        return [TopicPartition("t", tp.partition, -1001) for tp in partitions]

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        return (0, 0)

    def incremental_assign(self, partitions):
        self.calls.append("assign")

    def incremental_unassign(self, partitions):
        self.calls.append("unassign")


class Recorder(TransactionalProcessor):
    """Records hook calls in order, without real Kafka clients."""

    def __init__(self, producer=None, consumer=None):
        import logging

        self.log = logging.getLogger("test")
        self.producer = producer or FakeProducer()
        self.consumer = consumer or FakeConsumer()
        self.running = True
        self.in_transaction = False
        self.events = []
        self.commit_interval_sec = 0.05

    def handle(self, msg):
        self.events.append(("handle", msg.value()))

    def partitions_assigned(self, consumer, partitions):
        self.events.append("assigned")

    def partitions_revoked(self, partitions):
        self.events.append(("revoked", self.in_transaction))


class TP:
    def __init__(self, partition):
        self.partition = partition


def test_batch_is_handled_then_committed_with_offsets():
    processor = Recorder()
    processor.process_batch([FakeMessage(b"a"), FakeMessage(b"b")])
    assert processor.events == [("handle", b"a"), ("handle", b"b")]
    assert processor.producer.calls == ["begin", "offsets", "commit"]
    assert processor.in_transaction is False


def test_abortable_failure_aborts_instead_of_crashing():
    producer = FakeProducer(fail_commit_with=FakeError(abortable=True))
    processor = Recorder(producer=producer)
    processor.process_batch([FakeMessage(b"a")])
    assert producer.calls == ["begin", "offsets", "abort"]
    assert processor.in_transaction is False


def test_fatal_failure_is_raised():
    producer = FakeProducer(fail_commit_with=FakeError(abortable=False))
    processor = Recorder(producer=producer)
    try:
        processor.process_batch([FakeMessage(b"a")])
    except KafkaException:
        pass
    else:
        raise AssertionError("a fatal transaction error must not be swallowed")


def test_revoke_aborts_the_open_transaction_before_dropping_state():
    processor = Recorder()
    processor.in_transaction = True
    processor.on_revoke(processor.consumer, [TP(0)])
    assert processor.producer.calls == ["abort"]
    # The hook ran after the abort, so it saw no open transaction.
    assert processor.events == [("revoked", False)]
    assert processor.consumer.calls == ["unassign"]


def test_assign_hands_partitions_over_before_building_state():
    processor = Recorder()
    processor.on_assign(processor.consumer, [TP(1)])
    assert processor.consumer.calls == ["assign"]
    assert processor.events == ["assigned"]


def test_collect_batch_skips_partition_eof_markers():
    eof = FakeMessage(None, error=FakeError(code=KafkaError._PARTITION_EOF))
    consumer = FakeConsumer([FakeMessage(b"a"), eof, FakeMessage(b"b")])
    processor = Recorder(consumer=consumer)
    batch = processor.collect_batch()
    assert [m.value() for m in batch] == [b"a", b"b"]


def test_collect_batch_raises_on_fatal_errors():
    broken = FakeMessage(None, error=FakeError(code=KafkaError._FENCED, fatal=True))
    processor = Recorder(consumer=FakeConsumer([broken]))
    try:
        processor.collect_batch()
    except KafkaException:
        pass
    else:
        raise AssertionError("a fatal consumer error must not be swallowed")


def test_collect_batch_rides_out_errors_the_client_is_retrying():
    """A topic briefly unknown after creation must not stop the processor."""
    unknown = FakeMessage(None, error=FakeError(code=KafkaError.UNKNOWN_TOPIC_OR_PART))
    consumer = FakeConsumer([unknown, FakeMessage(b"a")])
    batch = Recorder(consumer=consumer).collect_batch()
    assert [m.value() for m in batch] == [b"a"]
