"""Writes edit war alerts from wiki.alerts into the alerts table.

Same guarantee as the edits sink: rows are committed before offsets, and the
insert is keyed by alert id, so a replayed batch changes nothing.
"""

import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.config import TOPIC_ALERTS  # noqa: E402
from processors.edit_war_detector import EditWarAlert  # noqa: E402
from sinks.db import insert_alerts  # noqa: E402
from sinks.postgres_sink import PostgresSink  # noqa: E402

GROUP_ID = "alerts-storage"


def build() -> PostgresSink:
    return PostgresSink(
        topic=TOPIC_ALERTS,
        group_id=GROUP_ID,
        decode=EditWarAlert.from_json,
        write=insert_alerts,
        label="alerts",
    )


def main() -> None:
    sink = build()
    signal.signal(signal.SIGINT, sink.stop)
    signal.signal(signal.SIGTERM, sink.stop)
    sink.run()


if __name__ == "__main__":
    main()
