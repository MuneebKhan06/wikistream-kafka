"""Replays recorded events into wiki.raw at a chosen speed.

Live traffic arrives at whatever rate the world produces, which is no way
to find a throughput limit. Replay pushes a recorded file through the same
pipeline either as fast as the brokers accept it, or at a multiple of the
original pace to reproduce a news spike.

Usage:
  python scripts/replay.py                      # sample file, as fast as possible
  python scripts/replay.py --speed 10           # 10x the original timing
  python scripts/replay.py --rate 5000          # fixed events per second
  python scripts/replay.py --acks 1 --loops 3   # weaker durability, 3 passes
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Producer  # noqa: E402

from common.config import TOPIC_RAW, producer_config  # noqa: E402
from common.metrics import RateMeter, setup_logging  # noqa: E402
from common.models import ParseError, event_id_of, parse_raw  # noqa: E402

log = setup_logging("replay")

DEFAULT_SAMPLE = Path("samples/recentchange_sample.jsonl")


def load(path: Path) -> list:
    """Read the file once so disk speed never limits the replay."""
    records = []
    skipped = 0
    with path.open("rb") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                raw = parse_raw(line)
                records.append((event_id_of(raw), line.strip(), raw.get("timestamp")))
            except ParseError:
                skipped += 1
    if skipped:
        log.warning("%d lines could not be parsed and were skipped", skipped)
    return records


def pace(records: list, index: int, started: float, args) -> None:
    """Sleep so the send matches the requested speed, if any."""
    if args.rate:
        target = started + index / args.rate
    elif args.speed:
        first = records[0][2]
        current = records[index][2]
        if not first or not current:
            return
        target = started + (current - first) / args.speed
    else:
        return

    delay = target - time.monotonic()
    if delay > 0:
        time.sleep(delay)


def producer_overrides(acks) -> dict:
    """An acks override also turns idempotence off, which requires acks=all.

    That is the point of the comparison: weaker acks means the producer
    gives up both the wait for the replicas and its duplicate protection.
    """
    if acks is None or str(acks) == "all":
        return {}
    return {"acks": str(acks), "enable.idempotence": False}


def replay(records: list, args) -> dict:
    overrides = producer_overrides(args.acks)
    if overrides:
        log.warning("acks=%s, so idempotence is disabled for this run", args.acks)
    producer = Producer(producer_config(**overrides))
    meter = RateMeter(log, f"replay acks={args.acks or 'all'}")
    delivered = {"ok": 0, "failed": 0}

    def on_delivery(err, _msg):
        if err is None:
            delivered["ok"] += 1
            meter.mark()
        else:
            delivered["failed"] += 1
            meter.mark_error()

    started = time.monotonic()
    for loop in range(args.loops):
        for index, (event_id, payload, _) in enumerate(records):
            pace(records, index, started, args)
            while True:
                try:
                    producer.produce(
                        args.topic,
                        key=event_id.encode("utf-8"),
                        value=payload,
                        on_delivery=on_delivery,
                    )
                    break
                except BufferError:
                    # Local queue is full, which means the brokers are the limit.
                    producer.poll(0.1)
            producer.poll(0)
        log.info("pass %d of %d sent", loop + 1, args.loops)

    producer.flush(120)
    elapsed = time.monotonic() - started
    meter.report(force=True)
    return {
        "sent": len(records) * args.loops,
        "delivered": delivered["ok"],
        "failed": delivered["failed"],
        "seconds": elapsed,
        "throughput": delivered["ok"] / elapsed if elapsed > 0 else 0.0,
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, default=DEFAULT_SAMPLE)
    parser.add_argument("--topic", default=TOPIC_RAW)
    parser.add_argument("--speed", type=float, help="multiple of the original pace")
    parser.add_argument("--rate", type=float, help="fixed events per second")
    parser.add_argument("--acks", help="override producer acks, for example 1")
    parser.add_argument("--loops", type=int, default=1)
    parser.add_argument("--json", action="store_true", help="print results as json")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    if not args.file.exists():
        log.error("no such file: %s", args.file)
        sys.exit(1)

    records = load(args.file)
    if not records:
        log.error("nothing to replay")
        sys.exit(1)

    log.info("replaying %d events x %d into %s", len(records), args.loops, args.topic)
    result = replay(records, args)

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        log.info(
            "%d delivered, %d failed in %.1fs (%.0f events/sec)",
            result["delivered"],
            result["failed"],
            result["seconds"],
            result["throughput"],
        )
    if result["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
