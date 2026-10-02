"""Measures producer throughput and live end to end latency.

throughput
    Replays distinct events as fast as the brokers accept them, once with
    acks=all and the idempotent producer, once with acks=1. The median of
    several runs is reported, since a single run on a shared machine varies.

live
    Runs the ingestor, cleaner and PostgreSQL sink on the live Wikimedia
    stream in an isolated sandbox. Latency is from the time an edit happened
    on the wiki to the time its row was committed in PostgreSQL, so it
    includes the source's own delay in publishing the event.

Usage:
  python scripts/benchmarks.py throughput
  python scripts/benchmarks.py live --seconds 180
  python scripts/benchmarks.py all

Results are printed and merged into benchmarks/performance.json.
"""

import argparse
import json
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.config import PROJECT_ROOT, STATE_DIR  # noqa: E402
from common.sandbox import Sandbox  # noqa: E402

RESULTS = PROJECT_ROOT / "benchmarks" / "performance.json"
SAMPLE_EVENTS = 2000

LATENCY = """
    SELECT
        COUNT(*),
        percentile_cont(0.50) WITHIN GROUP (ORDER BY lag),
        percentile_cont(0.95) WITHIN GROUP (ORDER BY lag),
        percentile_cont(0.99) WITHIN GROUP (ORDER BY lag),
        MAX(lag)
    FROM (
        SELECT EXTRACT(EPOCH FROM ingested_at - event_time) * 1000 AS lag FROM edits
    ) AS all_rows
"""

PER_SECOND = """
    SELECT MAX(c), AVG(c) FROM (
        SELECT date_trunc('second', event_time) AS s, COUNT(*) AS c FROM edits GROUP BY s
    ) AS per_second
"""


def throughput(loops: int, runs: int) -> dict:
    events = SAMPLE_EVENTS * loops
    result = {"events_per_run": events, "runs": runs}
    with Sandbox("bm", database=False) as box:
        box.python("admin/create_topics.py")
        for acks in ("all", "1"):
            rates = []
            for _ in range(runs):
                done = box.python(
                    "scripts/replay.py",
                    "--topic", box.topic("raw"),
                    "--loops", str(loops),
                    "--unique-ids",
                    "--acks", acks,
                    "--json",
                )
                outcome = json.loads(done.stdout[done.stdout.index("{"):])
                if outcome["failed"] or outcome["delivered"] != events:
                    raise RuntimeError(f"acks={acks}: {outcome}")
                rates.append(round(outcome["throughput"]))
            label = "acks_all_idempotent" if acks == "all" else "acks_1"
            result[label] = {
                "events_per_sec_median": round(statistics.median(rates)),
                "events_per_sec_runs": rates,
            }
    result["acks_1_speedup"] = round(
        result["acks_1"]["events_per_sec_median"]
        / result["acks_all_idempotent"]["events_per_sec_median"],
        2,
    )
    return result


def last_summary(log_text: str, marker: str) -> str:
    lines = [line for line in log_text.splitlines() if marker in line]
    return lines[-1].split("] ", 1)[1] if lines else ""


def live(seconds: int) -> dict:
    with Sandbox("bm") as box:
        checkpoint = STATE_DIR / f"{box.ns}-ingestor.json"
        try:
            box.python("admin/create_topics.py")
            for script in ("processors/cleaner.py", "sinks/postgres_sink.py", "ingestor/main.py"):
                box.start(script)
            box.wait_until(
                "the first rows to arrive",
                lambda: box.query("SELECT count(*) FROM edits")[0][0] > 0,
            )
            time.sleep(seconds)
            box.stop("ingestor/main.py")
            time.sleep(5)
            box.stop("processors/cleaner.py")
            box.stop("sinks/postgres_sink.py")

            rows, p50, p95, p99, worst = box.query(LATENCY)[0]
            peak, average = box.query(PER_SECOND)[0]
            cleaner = last_summary(box.log_text("processors/cleaner.py"), "stopped,")
            ingestor = last_summary(box.log_text("ingestor/main.py"), "stopped,")
        finally:
            checkpoint.unlink(missing_ok=True)

    return {
        "seconds_observed": seconds,
        "rows_stored": rows,
        "latency_ms": {
            "p50": round(p50),
            "p95": round(p95),
            "p99": round(p99),
            "max": round(worst),
        },
        "events_per_sec": {"average": round(float(average), 1), "peak": int(peak)},
        "cleaner": cleaner,
        "ingestor": ingestor,
    }


def save(results: dict) -> None:
    RESULTS.parent.mkdir(exist_ok=True)
    stored = json.loads(RESULTS.read_text()) if RESULTS.exists() else {}
    stored.update(results)
    RESULTS.write_text(json.dumps(stored, indent=2, sort_keys=True) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("benchmark", choices=["throughput", "live", "all"])
    parser.add_argument("--loops", type=int, default=50, help="passes of the sample per run")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--seconds", type=int, default=180)
    parser.add_argument("--no-save", action="store_true")
    args = parser.parse_args()

    results = {}
    if args.benchmark in ("throughput", "all"):
        results["producer_throughput"] = throughput(args.loops, args.runs)
    if args.benchmark in ("live", "all"):
        results["live_stream"] = live(args.seconds)
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for result in results.values():
        result["ran_at"] = stamp

    print(json.dumps(results, indent=2))
    if not args.no_save:
        save(results)


if __name__ == "__main__":
    main()
