"""One format for every record sent to wiki.dlq, whichever stage rejected it.

Each record says which stage gave up on it, why, where it came from, and
carries the original payload untouched, so a bad event can be inspected or
replayed without guessing how a particular stage wrapped it.
"""

import json
from datetime import datetime, timezone
from typing import Union


def decode_payload(payload: Union[bytes, str, None]) -> str:
    if payload is None:
        return ""
    if isinstance(payload, str):
        return payload
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError:
        return repr(payload)


def dlq_record(
    stage: str,
    reason: str,
    payload: Union[bytes, str, None],
    source: str,
    **position,
) -> bytes:
    """Build the value for a wiki.dlq message.

    `position` locates the original: partition and offset for a Kafka
    source, or the stream's event id for the live feed.
    """
    return json.dumps(
        {
            "stage": stage,
            "reason": reason,
            "source": source,
            "position": position,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "payload": decode_payload(payload),
        },
        ensure_ascii=False,
    ).encode("utf-8")
