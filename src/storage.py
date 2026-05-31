import hashlib
from sqlalchemy.exc import IntegrityError
from src.models.content import Content, ContentText
from src.models.chunk import Chunk
from src.db_sync import get_db_sync


class ContentStatus:
    PENDING = "pending"
    FETCHED = "fetched"
    CHUNKED = "chunked"
    INDEXED = "indexed"
    FAILED = "failed"


def save_text(url: str, text: str, title: str) -> tuple[str, bool]:
    """Сохраняет Content, дедуп по content_hash.

    Возвращает (doc_id, is_new). is_new=False — документ уже был, новый ContentText
    и чанки создавать не нужно, пайплайн индексации можно не запускать.
    Новая строка создаётся со status='fetched'.
    """
    content_hash = hashlib.blake2b(text.encode(), digest_size=16).digest()

    with get_db_sync() as db, db.begin():
        existing = db.query(Content).filter(Content.content_hash == content_hash).first()
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
                existing = db2.query(Content).filter(Content.content_hash == content_hash).first()
                return str(existing.id), False

        content_text = ContentText(
            text=text,
            content_id=content.id
        )
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
    """Возвращает {id, status, title, source_url} или None если не найден."""
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
        content_text = db.query(ContentText).filter(ContentText.content_id == doc_id).first().text
        return content_text


def save_chunks(chunks: list[dict], doc_id: str):
    with get_db_sync() as db, db.begin():
        chunks_obj = [
            Chunk(
                content_id=doc_id,
                chunk_index=i,
                text=c.page_content,
                text_hash=hashlib.blake2b(c.page_content.encode(), digest_size=16).digest(),
            )
            for i, c in enumerate(chunks)   
        ]
        db.add_all(chunks_obj)
        db.flush()
        chunks_ids = [str(c.id) for c in chunks_obj]
        db.commit()
        return chunks_ids


def _notify(session, request_id: str, **payload):
    payload["request_id"] = str(request_id)
    session.execute(
        text("SELECT pg_notify('request_update', :p)"),
        {"p": json.dumps(payload)},
    )