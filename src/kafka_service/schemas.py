from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TypeVar

from pydantic import BaseModel, Field

PayloadT = TypeVar("PayloadT", bound=BaseModel)


def _now() -> datetime:
    return datetime.now(UTC)


class Envelope[PayloadT](BaseModel):
    """Common wrapper for every pipeline message.

    request_id is carried through all stages - the client subscribes to status by it
    (WS / index.events) even before a doc_id exists.
    attempt is incremented when the message is re-sent from the DLQ.
    """

    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    request_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    type: str
    occurred_at: datetime = Field(default_factory=_now)
    producer: str = "cater"
    attempt: int = 0
    payload: PayloadT

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


class IndexRequest(BaseModel):
    url: str
    collection: str | None = None
    force: bool = False  # reindex even if the document is already INDEXED


class ContentFetched(BaseModel):
    doc_id: str
    url: str
    title: str | None = None
    is_new: bool = (
        True  # False - the content was already in the DB (dedup by content_hash)
    )
    collection: str | None = None
    force: bool = False


class ChunksReady(BaseModel):
    doc_id: str
    chunk_count: int
    collection: str | None = None
    force: bool = False


class IndexDone(BaseModel):
    doc_id: str
    vectors: int
    collection: str
    skipped: bool = False  # the document was already indexed, no work was done


class StatusEvent(BaseModel):
    """Status event for index.events: WS push, metrics, debugging."""

    stage: str
    status: str
    doc_id: str | None = None
    url: str | None = None
    detail: str | None = None


class StageFailed(BaseModel):
    """Body of a DLQ message: the original payload plus the failure context."""

    stage: str
    topic: str
    partition: int
    offset: int
    error: str
    error_type: str
    key: str | None = None
    raw: str | None = None  # raw body of the original message (utf-8, with replacement)
