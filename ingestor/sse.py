"""Minimal Server-Sent Events client for the Wikimedia stream.

Only the parts of the SSE spec the stream actually uses are handled:
`event:`, `data:` and `id:` fields, blank line ends an event, lines
starting with a colon are comments and keep the connection alive.
"""

import logging
from dataclasses import dataclass, field
from typing import Iterator, Optional

import requests

USER_AGENT = "wikistream-kafka/0.1 (https://github.com/MuneebKhan06/wikistream-kafka)"


@dataclass
class SSEEvent:
    event: str = "message"
    data: str = ""
    event_id: Optional[str] = None


@dataclass
class SSEParser:
    """Feeds lines in, yields complete events out."""

    event: str = "message"
    data_lines: list = field(default_factory=list)
    event_id: Optional[str] = None

    def feed(self, line: str) -> Optional[SSEEvent]:
        if line.startswith(":"):
            return None
        if line == "":
            return self._flush()

        field_name, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]

        if field_name == "data":
            self.data_lines.append(value)
        elif field_name == "event":
            self.event = value
        elif field_name == "id":
            self.event_id = value
        return None

    def _flush(self) -> Optional[SSEEvent]:
        if not self.data_lines:
            self.event = "message"
            return None
        event = SSEEvent(
            event=self.event,
            data="\n".join(self.data_lines),
            event_id=self.event_id,
        )
        self.event = "message"
        self.data_lines = []
        return event


def stream_events(
    url: str,
    headers: Optional[dict] = None,
    log: Optional[logging.Logger] = None,
    timeout: int = 60,
    session: Optional[requests.Session] = None,
) -> Iterator[SSEEvent]:
    """Yield events from one connection. Returns when the stream ends."""
    session = session or requests.Session()
    request_headers = {"Accept": "text/event-stream", "User-Agent": USER_AGENT}
    request_headers.update(headers or {})

    response = session.get(url, headers=request_headers, stream=True, timeout=timeout)
    response.raise_for_status()
    if log:
        resume = request_headers.get("Last-Event-ID")
        log.info("connected to %s%s", url, " (resuming)" if resume else "")

    parser = SSEParser()
    try:
        for line in response.iter_lines(decode_unicode=True):
            if line is None:
                continue
            event = parser.feed(line)
            if event is not None:
                yield event
    finally:
        response.close()
