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
    """Projection of index.events: where the request stands now, plus its timeline.

    id is the request_id from the Envelope, i.e. what the client received in the reply
    to POST /index and what it subscribes with over WS. The row exists for exactly one
    purpose: to serve a snapshot to whoever connected after the event (or after an API
    restart, or to another replica) - the Kafka status topic offers no such lookup by
    key.
    """

    __tablename__ = "request"

    # Appears only after fetch: before that the document is not in the DB yet.
    content_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("content.id", ondelete="SET NULL"),
        nullable=True,
    )
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    stage: Mapped[str | None] = mapped_column(String(32), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Offset of the last applied index.events event - for debugging lag.
    last_seq: Mapped[int] = mapped_column(BigInteger, nullable=False, default=-1)
    # Timeline [{event_id, stage, status, at, detail}] - the WS serves history from it
    # and deduplicates live events (by event_id).
    history: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)

    content: Mapped[Content | None] = relationship("Content", lazy="raise")

    __table_args__ = (Index("ix_request_content_id", "content_id"),)
