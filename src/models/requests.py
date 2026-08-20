from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.content import Content


class Request(UUIDMixin, TimestampMixin, Base):
    """Проекция index.events: «где сейчас запрос» + его таймлайн.

    id — это request_id из Envelope, то есть то, что клиент получил в ответе на
    POST /index и по чему подписывается на WS. Строка нужна ровно для одного:
    отдать снапшот тому, кто подключился позже события (или после рестарта API,
    или на другую реплику) — Kafka-топик статусов такой выборки по ключу не даёт.
    """

    __tablename__ = "request"

    # Появляется только после fetch: до него документа в БД ещё нет.
    content_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("content.id", ondelete="SET NULL"),
        nullable=True,
    )
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    stage: Mapped[str | None] = mapped_column(String(32), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Оффсет последнего применённого события index.events — для отладки лага.
    last_seq: Mapped[int] = mapped_column(BigInteger, nullable=False, default=-1)
    # Таймлайн [{event_id, stage, status, at, detail}] — по нему WS отдаёт историю
    # и дедупит live-события (по event_id).
    history: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)

    content: Mapped[Content | None] = relationship("Content", lazy="raise")

    __table_args__ = (Index("ix_request_content_id", "content_id"),)
