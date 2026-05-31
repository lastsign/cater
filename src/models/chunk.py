from __future__ import annotations

import uuid

from sqlalchemy import ForeignKey, Index, Integer, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDMixin
from src.models.content import Content


class Chunk(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "chunks"

    content_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("content.id", ondelete="CASCADE"),
        nullable=False,
    )
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    text_hash: Mapped[bytes] = mapped_column(nullable=False)
    token_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    content: Mapped["Content"] = relationship(back_populates="chunks")

    __table_args__ = (
        UniqueConstraint("content_id", "chunk_index", name="uq_chunks_content_index"),
        Index("ix_chunks_content_id", "content_id"),
        Index("ix_chunks_text_hash", "text_hash"),
    )
