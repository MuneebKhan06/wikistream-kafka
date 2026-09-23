"""Stores the last Wikimedia event id whose delivery Kafka confirmed.

The file is only advanced from a producer delivery callback, so a restart
resumes from an event Kafka already has. Writes go to a temp file in the
same directory and are renamed over the target, so a crash mid write
leaves either the old checkpoint or the new one, never a half written file.
"""

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

DEFAULT_PATH = Path(os.getenv("INGESTOR_CHECKPOINT", "state/ingestor.json"))


@dataclass
class Checkpoint:
    path: Path = DEFAULT_PATH
    last_event_id: Optional[str] = None
    events_confirmed: int = 0

    @classmethod
    def load(cls, path: Path = DEFAULT_PATH) -> "Checkpoint":
        path = Path(path)
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return cls(path=path)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return cls(path=path)
        if not isinstance(data, dict):
            return cls(path=path)
        last = data.get("last_event_id")
        count = data.get("events_confirmed", 0)
        return cls(
            path=path,
            last_event_id=last if isinstance(last, str) and last else None,
            events_confirmed=count if isinstance(count, int) else 0,
        )

    def advance(self, event_id: Optional[str], count: int = 1) -> None:
        """Record confirmed events in memory. Call save() to make it durable."""
        if event_id:
            self.last_event_id = event_id
        self.events_confirmed += count

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            {
                "last_event_id": self.last_event_id,
                "events_confirmed": self.events_confirmed,
            }
        )
        fd, tmp_name = tempfile.mkstemp(dir=str(self.path.parent), suffix=".tmp")
        tmp = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

    def resume_header(self) -> dict:
        """Last-Event-ID header so EventStreams resumes where we stopped."""
        if not self.last_event_id:
            return {}
        return {"Last-Event-ID": self.last_event_id}
