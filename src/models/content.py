from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from src.models.chunk import Chunk


class Content(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "content"

    source_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    content_hash: Mapped[bytes | None] = mapped_column(nullable=True)
    meta: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    text: Mapped[ContentText | None] = relationship(
        back_populates="content",
        uselist=False,
        cascade="all, delete-orphan",
    )
    chunks: Mapped[list[Chunk]] = relationship(
        back_populates="content",
        cascade="all, delete-orphan",
    )

    __table_args__ = (Index("ix_content_content_hash", "content_hash", unique=True),)


class ContentText(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "content_text"

    content_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("content.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    text: Mapped[str] = mapped_column(Text, nullable=False)

    content: Mapped[Content] = relationship(back_populates="text")
