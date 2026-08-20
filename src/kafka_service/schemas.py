from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TypeVar

from pydantic import BaseModel, Field

PayloadT = TypeVar("PayloadT", bound=BaseModel)


def _now() -> datetime:
    return datetime.now(UTC)


class Envelope[PayloadT](BaseModel):
    """Общая обёртка всех сообщений пайплайна.

    request_id тянется через все стадии — по нему клиент подписывается на статус
    (WS / index.events) ещё до того, как появится doc_id.
    attempt увеличивается при переотправке из DLQ.
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
    force: bool = False  # переиндексировать, даже если документ уже INDEXED


class ContentFetched(BaseModel):
    doc_id: str
    url: str
    title: str | None = None
    is_new: bool = True  # False — контент уже был в БД (дедуп по content_hash)
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
    skipped: bool = False  # документ уже был проиндексирован, работу не делали


class StatusEvent(BaseModel):
    """Статусное событие для index.events: WS-пуш, метрики, отладка."""

    stage: str
    status: str
    doc_id: str | None = None
    url: str | None = None
    detail: str | None = None


class StageFailed(BaseModel):
    """Тело сообщения в DLQ: исходный payload + контекст падения."""

    stage: str
    topic: str
    partition: int
    offset: int
    error: str
    error_type: str
    key: str | None = None
    raw: str | None = None  # сырое тело исходного сообщения (utf-8, с заменой)
