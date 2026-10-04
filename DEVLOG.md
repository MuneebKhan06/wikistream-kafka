# Development log

What was built each day, and more usefully, what broke and what the fix
taught. Most of the design decisions in the README were shaped by an entry
here.

## Day 1: The cluster and the foundations

- Three KRaft brokers, PostgreSQL and Kafka UI in Docker Compose. Every broker
  is also a controller, which later turned out to matter (Day 11).
- Topics as code: partitions, replication, retention and compaction live in
  `common/config.py`, and `admin/create_topics.py` applies them.
- Event parsing, built from real events pulled off the live stream rather than
  from the documentation.

## Day 2: Ingestion

- An SSE client, the ingestor, and a checkpoint written atomically after Kafka
  confirms delivery.
- **Bug found on the first live run:** the checkpoint's confirmed count read 521
  while 685 events were in the topic. Messages go to six partitions, so
  confirmations come back out of order, and the checkpoint may only advance to
  an event whose predecessors are all confirmed. One release can cover several
  messages, and the counter was counting it as one. Fixed and tested.
- Recorded 2,000 real events from 65 wikis as a test sample.

## Day 3: Exactly once cleaning

- A bounded, time bucketed dedup cache; the transform as pure functions; the
  transactional cleaner.
- **Crash test result:** `kill -9` mid transaction produced no duplicates and no
  loss, so the transactions hold. But 300 deliberately injected source
  duplicates got through after the restart: the dedup cache lives in memory
  and starts empty. Exactly once covered Kafka's own duplicates, not the
  source's.
- A second cleaner sharing a transactional id fenced the first. Instance ids
  became configurable.

## Day 4: Rebuilding state, and the PostgreSQL sink

- On assignment the cleaner now replays recent `wiki.raw` history into its
  cache. 300 injected duplicates, all dropped after a restart.
- **A trap caught before it shipped:** the first version replayed to the end of
  the partition. Records past the committed offset were never committed
  downstream, so loading their ids would make the cleaner discard them as
  duplicates on the very replay meant to recover them. The replay stops at the
  committed offset.
- The sink: rows first, offsets second, upserts on event id. A full replay
  inserted 0 new rows.
- The replay tool measured `acks=all` at a third of `acks=1` throughput.

## Day 5: Trending

- Minute windows that commit only the offsets an open window no longer needs,
  and totals written with "keep the larger" so a recount is harmless.
- The ingestor resumed from a day old checkpoint and replayed a day of backlog
  at 1,400 events per second, which turned out to be the best load test so far.
- **Found by reconciling against an independent count:** 4,587 page minutes
  were missing. Windows still open at shutdown were discarded unwritten, and
  the final log line said "0 windows open" after dropping them. A stop now
  writes open windows too, while holding the commit point behind them.
  Afterwards the table matched the topic exactly.
- The dead letter queue caught Wikimedia "canary" events with no type field.

## Day 6: Edit wars, and three improvements

- A detector for the three revert rule, a transactional processor writing
  alerts, warm up from history, and an alerts sink. 14 wars found in two hours
  of real traffic.
- **Simulating a crash mid war** showed why warm up matters: without it the
  restart raised a wrong alert with a different id that no downstream dedup
  could catch. With it, the original id came back.
- Revert detection in 12 languages: 564 reverts found became 827.
- One dead letter format for every stage.
- **A real data loss bug:** started from another directory, the ingestor could
  not find its checkpoint and began at the live edge, silently skipping
  everything since its last run. Paths are now anchored to the project root.

## Day 7: The compacted topic

- `wiki.page-latest`, with tombstones for deleted pages, and a reader that
  rebuilds every page's latest state from it. Compaction was observed running
  live.
- The cleaner and edit war processors shared most of their code; it became one
  transactional base class.
- **A failed delivery used to freeze the checkpoint forever** while the ingestor
  kept running. It now stops cleanly and resumes from before the failure.
- 20,250 events expired from `wiki.raw` before the cleaner read them: it had
  been stopped longer than the 24 hour retention.

## Day 8: Operations tooling

- Every consumer now reports, on assignment, exactly how many records expired
  before it read them.
- A broker failure helper. **The first measurements were wrong:** 17.6 s for a
  crash because each poll started a JVM, then 6.9 s for a graceful stop because
  the timer waited for `docker stop` to return. With a Python watcher running
  in parallel: about 8 s for a crash, 331 ms for a graceful stop.
- Replay was only rate limiting its first pass. Fixed.
- A latency and partition balance report: partition 0 carries 19.6% of traffic
  against an even 16.7%.

## Day 9: Isolation and robustness

- A namespace for topics, groups and transactional ids, so tests can run the
  real processes next to the real pipeline without touching it.
- **Arrival order is not event order:** 1,111 times in 609,204 records, an older
  edit of a page arrived after a newer one. Page state now orders by event time
  and rebuilds that guard from its own compacted output.
- Lag reporting crashed a processor during a leader change. Fixed and
  reproduced with a broker kill.
- **The end to end test failed one run in three,** and every failure was real:
  consumers crashing on a transient error, consumers starting before their
  topic existed, and consumers assigned zero partitions that never recovered.
  Traced through the client's debug logs to a group coordinator that had not
  learned the new topic yet. A watchdog fixed it: in 30 fresh subscriptions, 5
  stayed stuck without it and 0 with it.

## Day 10: The end to end test, CI, and a watermark bug

- The end to end test committed after 15 of 15 passing runs; CI went green on
  the first run on GitHub.
- **A watermark shared across partitions** made trending discard events from
  partitions it had not reached yet: replaying the backlog one partition after
  another, 79.4% were rejected as late. Per partition, only the deliberately
  stale sample was.
- Stream heartbeats are skipped instead of filling the dead letter queue: every
  entry from real traffic had been one.
- The sink reconnects when PostgreSQL restarts. Tested by restarting it under a
  276,000 row load: every row present exactly once.
- **A runner whose stop did not stop:** it recorded the shell's pid instead of
  Python's. Caught because the log had no shutdown line.

## Day 11: Failure tests and benchmarks

- Six failure tests in sandboxes, with crashes injected at exact points. All
  passed, and the numbers are in `docs/failure-tests.md`.
- **Two brokers down did not behave as planned.** The error is a timeout, not
  `NOT_ENOUGH_REPLICAS`, because losing two brokers also loses the controller
  quorum, and 98 of 200 "failed" writes appeared after recovery.
- **Cooperative rebalancing was not faster.** Eager paused everything for 0.1 s;
  cooperative paused only moved partitions, but for 3.2 s, one heartbeat
  interval. A one second heartbeat cut it to 1.05 s. The real case for
  cooperative is that the eager assignor forced 12 state rebuilds against 9.
- Scaling the cleaner out was correct but not faster on one saturated machine.

## Day 12: The dashboard

- A FastAPI service and a React app, six views, with charts built in SVG.
- The `edits` table gained an `is_revert` column and a partial index for it, and
  the 609,204 existing rows were backfilled.
- Rendering every view in headless Chrome caught layout bugs no unit test could:
  overlapping columns, clipped revision numbers, a table wider than its card.
- **Port 8000 belonged to another app on this machine,** and every "200" in the
  first check came from that app. The dashboard moved to 8050, and the runner
  now reports a process that dies at startup.
- The dashboard runs as one container: Node builds and tests the React app,
  then a slim Python image serves it with the API.

## Day 13: Finishing

- With the brokers stopped for hours, the ingestor's local queue filled and it
  crashed with an unhandled error. It now waits, and stops cleanly if the queue
  stays full. Tested against the live stream with the brokers down.
- The dashboard counted dead letter offsets, not records: each record in that
  transactional topic is followed by a commit marker. It showed 10 for 5.
- This documentation.
