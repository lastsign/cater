import hashlib
import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from src.db import SessionLocal
from src.db_sync import get_db_sync
from src.kafka_service.events import is_final, supersedes
from src.models.chunk import Chunk
from src.models.content import Content, ContentText
from src.models.requests import Request


class ContentStatus:
    PENDING = "pending"
    FETCHED = "fetched"
    CHUNKED = "chunked"
    INDEXED = "indexed"
    FAILED = "failed"


def save_text(url: str, text: str, title: str) -> tuple[str, bool]:
    """Stores a Content row, deduplicated by content_hash.

    Returns (doc_id, is_new). is_new=False means the document already existed, so a new
    ContentText and chunks need not be created and the indexing pipeline can be skipped.
    A new row is created with status='fetched'.
    """
    content_hash = hashlib.blake2b(text.encode(), digest_size=16).digest()

    with get_db_sync() as db, db.begin():
        existing = (
            db.query(Content).filter(Content.content_hash == content_hash).first()
        )
        if existing is not None:
            return str(existing.id), False

        content = Content(
            source_type="huggingface.co",
            source_url=url,
            title=title,
            content_hash=content_hash,
            status=ContentStatus.FETCHED,
        )
        db.add(content)
        try:
            db.flush()
        except IntegrityError:
            db.rollback()
            with get_db_sync() as db2, db2.begin():
                existing = (
                    db2.query(Content)
                    .filter(Content.content_hash == content_hash)
                    .first()
                )
                return str(existing.id), False

        content_text = ContentText(text=text, content_id=content.id)
        db.add(content_text)
        db.flush()
        content_id = str(content.id)
        db.commit()
        return content_id, True


def set_status(doc_id: str, status: str) -> None:
    with get_db_sync() as db, db.begin():
        db.query(Content).filter(Content.id == doc_id).update({Content.status: status})
        db.commit()


def get_status(doc_id: str) -> dict | None:
    """Returns {id, status, title, source_url}, or None if not found."""
    with get_db_sync() as db:
        c = db.query(Content).filter(Content.id == doc_id).first()
        if c is None:
            return None
        return {
            "id": str(c.id),
            "status": c.status,
            "title": c.title,
            "source_url": c.source_url,
        }


def load_text(doc_id: str):
    with get_db_sync() as db, db.begin():
        content_text = (
            db.query(ContentText).filter(ContentText.content_id == doc_id).first().text
        )
        return content_text


def save_chunks(chunks: list[dict], doc_id: str):
    with get_db_sync() as db, db.begin():
        chunks_obj = [
            Chunk(
                content_id=doc_id,
                chunk_index=i,
                text=c.page_content,
                text_hash=hashlib.blake2b(
                    c.page_content.encode(), digest_size=16
                ).digest(),
            )
            for i, c in enumerate(chunks)
        ]
        db.add_all(chunks_obj)
        db.flush()
        chunks_ids = [str(c.id) for c in chunks_obj]
        db.commit()
        return chunks_ids


def count_chunks(doc_id: str) -> int:
    with get_db_sync() as db:
        return db.query(Chunk).filter(Chunk.content_id == doc_id).count()


def existing_chunk_ids(chunk_ids: list[str]) -> set[str]:
    """Which of the given ids are still alive in the DB. Used to find orphaned Qdrant points."""
    if not chunk_ids:
        return set()
    with get_db_sync() as db:
        rows = db.query(Chunk.id).filter(Chunk.id.in_(chunk_ids)).all()
        return {str(r[0]) for r in rows}


# --- request status projection (index.events -> request) ---------------------
#
# The client receives the live status stream over WS straight from Kafka. This table
# is only needed for the snapshot: the client connects AFTER fetch has already run
# (or after an API restart, or to another replica) and must still see the doc_id and
# the current status. Kafka cannot answer "give me the events of this request_id".

HISTORY_MAX = 64


async def create_request(request_id: uuid.UUID, url: str) -> None:
    """Puts the request into pending right at submit time - before the first Kafka event.

    Without this, a WS opened immediately after POST /index would get no snapshot (the
    row does not exist yet) and would hang silently until the first event.
    """
    async with SessionLocal() as db, db.begin():
        if await db.get(Request, request_id) is not None:
            return
        db.add(
            Request(id=request_id, url=url, status=ContentStatus.PENDING, last_seq=-1)
        )


async def record_status_event(view: dict) -> None:
    """Applies one flat event (kafka_service.events.status_view) to the row.

    Idempotent: repeating the same event_id changes nothing - Kafka's at-least-once
    delivery guarantees repeats. The row is taken FOR UPDATE because two projector
    instances may own different partitions of index.events and write to the same row
    (a fetch failure is keyed by url, everything else by doc_id).
    """
    request_id = uuid.UUID(view["request_id"])
    entry = {
        "event_id": view["event_id"],
        "stage": view["stage"],
        "status": view["status"],
        "at": view["at"],
        "detail": view["detail"],
    }

    async with SessionLocal() as db, db.begin():
        row = await db.get(Request, request_id, with_for_update=True)
        if row is None:
            row = Request(
                id=request_id, status=ContentStatus.PENDING, last_seq=-1, history=[]
            )
            db.add(row)
            await db.flush()

        if any(e.get("event_id") == entry["event_id"] for e in row.history):
            return

        # Assign a new list: SQLAlchemy does not track JSONB mutations on its own.
        row.history = (row.history + [entry])[-HISTORY_MAX:]
        if view["doc_id"]:
            row.content_id = uuid.UUID(view["doc_id"])
        if view["url"]:
            row.url = view["url"]
        if view["seq"] is not None:
            row.last_seq = max(row.last_seq, view["seq"])
        if supersedes(row.status, view["status"]):
            row.stage = view["stage"]
            row.status = view["status"]
            row.detail = view["detail"]


async def load_request_snapshot(request_id: uuid.UUID) -> dict:
    """Snapshot for a freshly opened WS.

    If the row is missing we return status=unknown rather than a 404: events may still
    arrive later, so it is worth keeping the connection.
    """
    async with SessionLocal() as db:
        row = await db.get(Request, request_id)
        if row is None:
            return {
                "type": "snapshot",
                "request_id": str(request_id),
                "status": "unknown",
                "final": False,
                "history": [],
            }
        return {
            "type": "snapshot",
            "request_id": str(row.id),
            "doc_id": str(row.content_id) if row.content_id else None,
            "url": row.url,
            "stage": row.stage,
            "status": row.status,
            "detail": row.detail,
            "final": is_final(row.status),
            "seq": row.last_seq,
            "history": list(row.history),
            "updated_at": row.updated_at_dt.isoformat() if row.updated_at_dt else None,
        }


async def load_content_status(doc_id: uuid.UUID) -> dict | None:
    """Async counterpart of get_status - for the HTTP status endpoint by doc_id."""
    async with SessionLocal() as db:
        c = (
            await db.execute(select(Content).where(Content.id == doc_id))
        ).scalar_one_or_none()
        if c is None:
            return None
        return {
            "doc_id": str(c.id),
            "status": c.status,
            "title": c.title,
            "source_url": c.source_url,
        }
