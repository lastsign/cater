"""Projection of index.events messages into a flat shape for WS and for the DB.

The Kafka envelope (`Envelope[StatusEvent]`) is awkward to hand to a browser and to
store in Postgres: the very same shape is needed both by the pump (WS) and by the
projector (snapshot), so it lives here instead of in two places.
"""

from __future__ import annotations

import logging

from src.kafka_service.schemas import Envelope, StatusEvent

log = logging.getLogger(__name__)

# States after which nothing more will arrive for this request_id:
# indexed - the document is in Qdrant, skipped - it was already indexed, failed - DLQ.
TERMINAL_STATUSES = frozenset({"indexed", "skipped", "failed"})

# Progress ordering. The projector needs it: events of two stages may arrive out of
# order (a fetch failure goes to index.events keyed by url, the rest are keyed by
# doc_id, i.e. land in a different partition).
STATUS_RANK = {"pending": 0, "fetched": 1, "chunked": 2, "indexed": 3}


def is_final(status: str | None) -> bool:
    return status in TERMINAL_STATUSES


def status_view(env: Envelope[StatusEvent], seq: int | None = None) -> dict:
    """Flat status event. seq is the offset in index.events (for dedup/debugging)."""
    p = env.payload
    return {
        "type": "status",
        "request_id": str(env.request_id),
        "event_id": str(env.event_id),
        "stage": p.stage,
        "status": p.status,
        "doc_id": p.doc_id,
        "url": p.url,
        "detail": p.detail,
        "at": env.occurred_at.isoformat(),
        "seq": seq,
        "final": is_final(p.status),
    }


def parse_status_event(raw: bytes | None, seq: int | None = None) -> dict | None:
    """Message bytes -> flat event. None if the body is not a status event or is broken."""
    try:
        env = Envelope[StatusEvent].model_validate_json(raw or b"")
    except Exception:
        log.exception("bad status event at seq=%s", seq)
        return None
    return status_view(env, seq)


def supersedes(old_status: str | None, new_status: str | None) -> bool:
    """Whether to move the request status forward.

    A terminal state is always applied (it is the end of the story), progress only
    moves forward along STATUS_RANK, but after failed we allow starting over: a DLQ
    replay goes through the same stages with the same request_id.
    """
    if old_status is None or old_status == "failed":
        return True
    if is_final(new_status):
        return True
    return STATUS_RANK.get(new_status, -1) > STATUS_RANK.get(old_status, -1)
