"""Runs a real pipeline process that dies at one precise point.

A random kill -9 only sometimes lands in the window a failure test is
about. These launchers run the production classes unchanged except for one
overridden step, which exits the process without any cleanup at exactly the
moment under test. os._exit skips handlers, finally blocks and client
shutdown, so to Kafka and PostgreSQL it is indistinguishable from a crash.

The hooks live here rather than in the processors, so production code
carries no test switches.

Usage:
  python scripts/faults.py cleaner-mid-transaction [BATCH]
      dies after producing a batch's output and sending its offsets to the
      transaction, before committing it
  python scripts/faults.py sink-after-write [BATCH]
      dies after PostgreSQL commits a batch, before Kafka offsets are committed
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

CRASH_EXIT_CODE = 86


def cleaner_mid_transaction(batch_number: int) -> None:
    from processors.cleaner import Cleaner, log

    class CrashingCleaner(Cleaner):
        batches = 0

        def process_batch(self, batch: list) -> None:
            self.batches += 1
            if self.batches < batch_number:
                return super().process_batch(batch)

            self.producer.begin_transaction()
            for msg in batch:
                self.handle(msg)
            positions = self.consumer.position(self.consumer.assignment())
            self.producer.send_offsets_to_transaction(
                positions, self.consumer.consumer_group_metadata()
            )
            # Push the output to the brokers so it is really in the log,
            # uncommitted, when the process disappears.
            self.producer.flush(30)
            log.warning(
                "fault: dying mid transaction with %d records produced and uncommitted",
                len(batch),
            )
            os._exit(CRASH_EXIT_CODE)

    CrashingCleaner().run()


def sink_after_write(batch_number: int) -> None:
    from sinks.db import insert_edits
    from sinks.postgres_sink import PostgresSink, log

    calls = {"n": 0}

    def write_then_die(connection, events):
        written = insert_edits(connection, events)
        calls["n"] += 1
        if calls["n"] >= batch_number:
            # insert_edits has committed its transaction by the time it
            # returns, and the sink has not yet committed offsets.
            log.warning(
                "fault: dying after PostgreSQL committed %d rows, before the offset commit",
                written,
            )
            os._exit(CRASH_EXIT_CODE)
        return written

    PostgresSink(write=write_then_die).run()


FAULTS = {
    "cleaner-mid-transaction": cleaner_mid_transaction,
    "sink-after-write": sink_after_write,
}


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in FAULTS:
        print(__doc__)
        sys.exit(2)
    batch_number = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    FAULTS[sys.argv[1]](batch_number)


if __name__ == "__main__":
    main()
