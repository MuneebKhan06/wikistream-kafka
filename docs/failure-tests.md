# Failure tests

Each test breaks one thing at a precise moment, lets the pipeline recover, then
reads the data back from Kafka and PostgreSQL and checks it. A test passes on
what the data shows, never on the absence of an exception.

All six ran against the real three broker cluster, recorded on 2 October 2026.
The raw results are in [`benchmarks/failure_tests.json`](../benchmarks/failure_tests.json).
To run them again:

```
python scripts/failure_tests.py              # all six, about eight minutes
python scripts/failure_tests.py sink-crash   # one
python scripts/failure_tests.py --list
```

## How the tests are isolated

Every test runs in a **sandbox** ([`common/sandbox.py`](../common/sandbox.py)):
a fresh namespace gives it its own topics, consumer groups and transactional
ids, and it gets its own PostgreSQL database. The processes are the real ones,
pointed at the sandbox through the environment. This matters for more than
tidiness. A test cleaner reusing the production transactional id would fence
the real cleaner, and a test consumer joining a production group would take
its partitions.

A random `kill -9` only sometimes lands in the window a test is about, so the
crash tests use [`scripts/faults.py`](../scripts/faults.py). It runs the
production classes unchanged except for one overridden step, which calls
`os._exit` at exactly the moment under test. No handler, `finally` block or
client shutdown runs: to Kafka and PostgreSQL it is a crash. The hooks live in
the test tooling, so production code has no test switches.

## Summary

| Test | What was broken | Result |
|---|---|---|
| [Cleaner crash](#cleaner-crash-mid-transaction) | Cleaner exits mid transaction | 658 uncommitted records left in the log, none ever visible; 2,000 of 2,000 events, 0 duplicates |
| [Sink crash](#sink-crash-after-the-database-write) | Sink exits after the database commit, before the offset commit | 500 rows reprocessed and skipped; 2,000 unique rows |
| [Ingestor crash](#ingestor-crash-on-the-live-stream) | Ingestor killed with SIGKILL on the live stream | 0 events missing from the source's own sequence; 31 duplicates (1.3%) |
| [One broker down](#one-broker-down-while-producing) | Broker killed while producing at 2,000/s | 30,000 sent, acknowledged and stored; leadership moved in 8.9 s |
| [Two brokers down](#two-brokers-down) | Two of three brokers killed | 0 writes acknowledged, 0 acknowledged writes lost |
| [Scale out](#consumer-scale-out) | Cleaners go from one instance to three under a backlog | All 6 partitions owned, 2 each; 120,000 of 120,000 events |

## Cleaner crash mid transaction

**What it checks.** The cleaner writes its output and commits its input
offsets in one Kafka transaction. If it dies in the middle, no partial output
may become visible and no event may be written twice.

**Method.** 6,000 records go into `raw`: the 2,000 event sample three times
over, so two thirds of the input are duplicates. The faulty cleaner processes
its first batch normally. On the second it produces the batch's output, sends
the offsets to the transaction, flushes so the records are really in the log,
and exits before `commit_transaction()`. A normal cleaner then starts with the
same transactional id and runs until its group has caught up.

**Evidence.**

| Measure | Value |
|---|---|
| Records in `clean` visible to a `read_committed` reader after the crash | 680 |
| Records actually written to the log | 1,338 |
| Uncommitted records left behind by the crash | 658 |
| Records in `clean` after the restart | 2,000 |
| Unique event ids among them | 2,000 |
| Duplicate event ids | 0 |

The 658 records were in the log but never visible to committed readers. When
the restarted cleaner initialised its transactions, the coordinator aborted
the open transaction for that id, so those records stay invisible for good.
The restart then reprocessed the second batch from the last committed offset.

**What this proves and what it does not.** It proves the Kafka to Kafka step
is exactly once, including across a crash at the worst moment. Removing source
duplicates, the other two thirds of the input, is a separate mechanism: an in
memory cache per partition, which is rebuilt on restart by replaying recent
history (see the DEVLOG, Days 3 and 4, for why that rebuild must stop at the
committed offset).

## Sink crash after the database write

**What it checks.** Kafka transactions stop at the Kafka boundary. The sink
commits a batch to PostgreSQL first and Kafka offsets second, so a crash in
between must cause reprocessing, never loss, and reprocessing must not create
duplicate rows.

**Method.** 2,000 events are cleaned into `clean`. The faulty sink writes its
first batch and exits after PostgreSQL has committed it, before committing
offsets. A normal sink then runs until every event is stored, and is stopped
gracefully.

**Evidence.**

| Measure | Value |
|---|---|
| Rows in PostgreSQL after the crash | 500 |
| Partitions with offsets committed at that point | 0 |
| Rows the restarted sink found already present and skipped | 500 |
| Rows after the restart | 2,000 |
| Unique event ids | 2,000 |
| Records left uncommitted after the restart | 0 |

The restarted sink read the same 500 events again, and the insert's
`ON CONFLICT (event_id) DO NOTHING` skipped every one. At least once delivery
plus an idempotent write gives exactly once results in the table.

A note on measuring this: `clean` is written in transactions, so each
partition ends with a commit marker that takes an offset. A consumer that has
read everything commits the offset just before that marker. "Caught up" means
one short of the end, not equal to it, and the test accounts for that.

## Ingestor crash on the live stream

**What it checks.** The ingestor saves the stream's position only after Kafka
confirms delivery. After a crash it must resume from that position, so no
event is skipped. Some events will arrive twice, which is accepted by design.

**Method.** The ingestor reads the live Wikimedia stream for 25 seconds, is
killed with `SIGKILL`, and is started again for another 25 seconds.

**Evidence.**

| Measure | Value |
|---|---|
| Events confirmed in the checkpoint at the crash | 1,165 |
| Records in `raw` at the crash | 1,196 |
| Resumed from the checkpoint | yes |
| Records in `raw` after the restart | 2,459 |
| Unique events | 2,428 |
| Duplicates from the restart | 31 (1.3%) |
| Events missing from the source's offset sequence | 0 |

The numbers agree with each other: 1,196 records reached Kafka but the
checkpoint had confirmed 1,165, and exactly the difference, 31, arrived twice.
Gaps are measured against the source itself: Wikimedia stamps every event with
its own topic, partition and offset, and those offsets are contiguous across
the restart.

**Why the checkpoint is careful.** Messages go to six partitions, so delivery
confirmations come back out of order. The checkpoint only moves to an event
whose predecessors are all confirmed, otherwise a crash would skip the gaps.

## One broker down while producing

**What it checks.** With replication factor 3, `min.insync.replicas=2` and
`acks=all`, losing one broker must not lose any acknowledged write.

**Method.** A producer sends 30,000 events at 2,000 per second. Five seconds in,
broker 2 is killed with `SIGKILL`. Afterwards the broker is restarted and the
cluster is rebalanced.

**Evidence.**

| Measure | Value |
|---|---|
| Sent | 30,000 |
| Acknowledged | 30,000 |
| Failed | 0 |
| Records in the topic | 30,000 |
| Time until no partition was led by the dead broker | 8.9 s |
| Time for the restarted broker's replicas to catch up | 11.5 s |

Exactly 30,000 records, not more: the idempotent producer's retries during the
leader change created no duplicates.

**On the leadership timing.** After a crash the KRaft controller cannot know
the broker is gone until its heartbeats have been missing for the broker
session timeout, nine seconds by default. Measured separately with
[`scripts/kill_broker.sh`](../scripts/kill_broker.sh): a hard kill moves
leadership in about 7 to 8 seconds when idle, while a graceful stop, where
the broker hands its partitions over before exiting, takes 331 ms. Early
measurements were badly off, first because each poll started a JVM, then
because the timer waited for `docker stop` to return; the final numbers come
from a Python watcher polling metadata every 100 ms in parallel with the stop.

## Two brokers down

**What it checks.** With two of three brokers gone, writes must be refused
rather than silently accepted by a single copy.

**Method.** 100 writes are made and confirmed. Brokers 2 and 3 are killed.
200 more writes are attempted with `acks=all` and a 20 second delivery
timeout. Then both brokers are restarted and the topic is read back.

**Evidence.**

| Measure | Value |
|---|---|
| Acknowledged before the outage | 100 of 100 |
| Acknowledged during the outage | 0 of 200 |
| Error on every refused write | `_MSG_TIMED_OUT` |
| Acknowledged writes missing after recovery | 0 |
| Refused writes that appeared in the topic after recovery | 98 |

**Two findings that differ from the original plan.**

First, the error is a timeout, not `NOT_ENOUGH_REPLICAS`. Each broker here is
also a KRaft controller, so losing two brokers also loses the controller
quorum. With no controller, nothing can shrink the surviving partitions'
in-sync replica lists, so the leader does not know it is short of replicas. It
appends the write and waits for replication that never comes.

Second, 98 of the 200 refused writes appeared in the topic once the other
brokers returned: the surviving leader had appended them, and they replicated
on recovery. **A timeout means the outcome is unknown, not that the write did
not happen.** This is why the ingestor stops when a delivery fails instead of
moving on, and re-reads from its checkpoint on restart; the cleaner's
deduplication by event id then removes anything that was written twice.

## Consumer scale out

**What it checks.** Adding consumer instances under load must redistribute
partitions without losing or duplicating events.

**Method.** 120,000 distinct events are loaded into `raw`. One cleaner starts;
a second joins 12 seconds later and a third 12 seconds after that. The test
waits until the group has drained the backlog, then checks ownership from the
processes' own logs and the output from the topic. It runs twice: once with
the cooperative-sticky assignor the pipeline uses, once with the eager `range`
assignor for comparison.

**Evidence.**

| Measure | Cooperative-sticky | Eager (`range`) |
|---|---|---|
| Partitions owned at the end | 2, 2, 2 | 2, 2, 2 |
| Events in `clean` | 120,000 | 120,000 |
| Unique event ids | 120,000 | 120,000 |
| State rebuilds during the run | 9 | 12 |
| Partitions revoked during the run | 5 | 12 |
| Seconds to drain | 45.4 | 37.4 |

Both assignors kept every partition owned and every event exactly once. The
difference is in how much state had to be thrown away: the eager assignor
revokes every partition from every member on each rebalance, and each cleaner
then rebuilds its deduplication cache for everything it is given back. That
cost, not raw pause time, is why the pipeline uses cooperative-sticky; see
[Rebalancing](../README.md#rebalancing) in the README for the pause
measurements.

**Throughput did not scale on this machine.** The drain rate with one, two
and three cleaners was 1,839, 2,586 and 3,030 events per second in this run,
but an earlier run measured 3,462, 2,500 and 3,214. The host has eight cores
and was saturated (load average 7.1) with three brokers, PostgreSQL and the
consumers all sharing it. This test shows that scaling out is correct; it does
not show that it is faster. That would need brokers and consumers on separate
machines.

## Found by running the pipeline rather than by these tests

Some failure modes only appeared in longer live runs, and were fixed and
covered by unit tests:

- **Records expiring unread.** A group away longer than `raw`'s 24 hour
  retention finds its committed offset deleted, and the client quietly moves it
  forward. 20,250 events were lost this way on Day 7. Every consumer now checks
  its committed offsets against the log start on assignment and logs exactly
  how many records expired.
- **A crash in lag reporting.** A routine lag query raised
  `NOT_LEADER_FOR_PARTITION` during a leader change and took a processor down.
  Lag reporting now skips a partition it cannot read.
- **Consumers stuck with no partitions.** A group formed right after its topic
  was created could be assigned zero partitions and never recover, because the
  group coordinator had not learned the topic yet. Seen in about one in ten
  fresh subscriptions; a watchdog now detects partitions nobody owns and forces
  a rejoin.
- **The ingestor crashing on a full queue.** With the brokers down for hours,
  the producer's local queue filled and `produce()` raised an unhandled error.
  It now waits, and if the queue stays full, stops through the same path as a
  failed delivery.
