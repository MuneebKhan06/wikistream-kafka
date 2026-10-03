"""A few seconds of caching per question, shared by every client.

The dashboard refreshes on a timer, and several tabs or viewers would each
ask the same expensive question. Answers are kept for a short time, so the
database sees one query per interval however many people are watching.
"""

import threading
import time
from typing import Callable


class TTLCache:
    def __init__(self, seconds: float, clock: Callable[[], float] = time.monotonic):
        self.seconds = seconds
        self.clock = clock
        self._entries = {}
        self._lock = threading.Lock()

    def get(self, key, compute: Callable):
        now = self.clock()
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None and now - entry[0] < self.seconds:
                return entry[1]
        value = compute()
        with self._lock:
            self._entries[key] = (self.clock(), value)
            if len(self._entries) > 256:
                oldest = min(self._entries, key=lambda k: self._entries[k][0])
                del self._entries[oldest]
        return value
