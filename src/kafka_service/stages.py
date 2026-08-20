"""Стадии пайплайна индексации: fetch -> chunk -> embed.

Каждая стадия — чистая функция «payload на входе, список сообщений на выходе».
Kafka-механику (коммиты, ретраи, DLQ) знает только worker.py, стадии её не видят —
их можно вызывать напрямую в тестах.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass

import httpx
from pydantic import BaseModel

from src.kafka_service.config import (
    DEFAULT_COLLECTION,
    EMBED_MAX_POLL_INTERVAL_MS,
    GROUP_CHUNK,
    GROUP_EMBED,
    GROUP_FETCH,
    HTTP_TIMEOUT_S,
    TOPIC_CHUNKS_READY,
    TOPIC_CONTENT_FETCHED,
    TOPIC_INDEX_DONE,
    TOPIC_INDEX_EVENTS,
    TOPIC_INDEX_REQUESTS,
)
from src.kafka_service.schemas import (
    ChunksReady,
    ContentFetched,
    Envelope,
    IndexDone,
    IndexRequest,
    StatusEvent,
)
from src.storage import (
    ContentStatus,
    count_chunks,
    get_status,
    load_text,
    save_chunks,
    save_text,
    set_status,
)

log = logging.getLogger(__name__)

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200


@dataclass(frozen=True)
class Emit:
    """Сообщение, которое стадия просит отправить дальше."""

    topic: str
    payload: BaseModel
    type: str
    key: str | None = None


@dataclass(frozen=True)
class Stage:
    name: str
    topics: tuple[str, ...]
    group_id: str
    payload_model: type[BaseModel]
    handler: Callable[[BaseModel, Envelope], Sequence[Emit]]
    max_attempts: int = 3
    consumer_overrides: dict | None = None


def _status_emit(
    stage: str,
    status: str,
    doc_id: str | None,
    url: str | None = None,
    detail: str | None = None,
) -> Emit:
    return Emit(
        topic=TOPIC_INDEX_EVENTS,
        payload=StatusEvent(
            stage=stage, status=status, doc_id=doc_id, url=url, detail=detail
        ),
        type="index.status",
        key=doc_id or url,
    )


def _already_indexed(doc_id: str) -> bool:
    info = get_status(doc_id)
    return bool(info and info["status"] == ContentStatus.INDEXED)


# --- fetch ------------------------------------------------------------------


def handle_index_request(payload: IndexRequest, env: Envelope) -> list[Emit]:
    from bs4 import BeautifulSoup

    resp = httpx.get(payload.url, timeout=HTTP_TIMEOUT_S, follow_redirects=True)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    title = soup.title.string if soup.title else None
    full_text = soup.get_text(strip=True)

    doc_id, is_new = save_text(payload.url, full_text, title or "")
    log.info("fetched url=%s doc_id=%s is_new=%s", payload.url, doc_id, is_new)

    return [
        Emit(
            topic=TOPIC_CONTENT_FETCHED,
            payload=ContentFetched(
                doc_id=doc_id,
                url=payload.url,
                title=title,
                is_new=is_new,
                collection=payload.collection,
                force=payload.force,
            ),
            type="content.fetched",
            # Ключ = doc_id: все сообщения одного документа ложатся в одну партицию,
            # значит стадии обрабатывают его строго по порядку и без гонок.
            key=doc_id,
        ),
        _status_emit("fetch", ContentStatus.FETCHED, doc_id, payload.url),
    ]


# --- chunk ------------------------------------------------------------------


def handle_content_fetched(payload: ContentFetched, env: Envelope) -> list[Emit]:
    collection = payload.collection or DEFAULT_COLLECTION

    if not payload.force and _already_indexed(payload.doc_id):
        log.info("chunk skipped, already indexed doc_id=%s", payload.doc_id)
        return [
            Emit(
                topic=TOPIC_INDEX_DONE,
                payload=IndexDone(
                    doc_id=payload.doc_id,
                    vectors=0,
                    collection=collection,
                    skipped=True,
                ),
                type="index.done",
                key=payload.doc_id,
            ),
            _status_emit("chunk", "skipped", payload.doc_id, payload.url),
        ]

    # Чанки уже есть — повторная доставка или force. Текст тот же (дедуп по
    # content_hash), поэтому переразбивать нечего, а save_chunks второй раз
    # упал бы на uq_chunks_content_index. Просто пропускаем документ дальше.
    existing = count_chunks(payload.doc_id)
    if existing:
        log.info("chunk reused doc_id=%s chunks=%d", payload.doc_id, existing)
        chunk_count = existing
    else:
        from langchain_text_splitters import RecursiveCharacterTextSplitter

        splitter = RecursiveCharacterTextSplitter(
            chunk_size=CHUNK_SIZE,
            chunk_overlap=CHUNK_OVERLAP,
            length_function=len,
            add_start_index=True,
        )
        chunks = splitter.create_documents([load_text(payload.doc_id)])
        chunk_count = len(save_chunks(chunks, payload.doc_id))
        set_status(payload.doc_id, ContentStatus.CHUNKED)
        log.info("chunked doc_id=%s chunks=%d", payload.doc_id, chunk_count)

    return [
        Emit(
            topic=TOPIC_CHUNKS_READY,
            payload=ChunksReady(
                doc_id=payload.doc_id,
                chunk_count=chunk_count,
                collection=collection,
                force=payload.force,
            ),
            type="chunks.ready",
            key=payload.doc_id,
        ),
        _status_emit("chunk", ContentStatus.CHUNKED, payload.doc_id, payload.url),
    ]


# --- embed ------------------------------------------------------------------


def handle_chunks_ready(payload: ChunksReady, env: Envelope) -> list[Emit]:
    from src.embedder.indexer import index_document, reindex_document

    collection = payload.collection or DEFAULT_COLLECTION

    if not payload.force and _already_indexed(payload.doc_id):
        log.info("embed skipped, already indexed doc_id=%s", payload.doc_id)
        return [
            Emit(
                topic=TOPIC_INDEX_DONE,
                payload=IndexDone(
                    doc_id=payload.doc_id,
                    vectors=0,
                    collection=collection,
                    skipped=True,
                ),
                type="index.done",
                key=payload.doc_id,
            ),
            _status_emit("embed", "skipped", payload.doc_id),
        ]

    # Qdrant upsert идёт по id чанка, поэтому повтор стадии перезаписывает точки,
    # а не дублирует их. force дополнительно сносит старые точки документа —
    # нужно, если чанков стало меньше и «хвост» иначе остался бы в коллекции.
    vectors = (
        reindex_document(payload.doc_id, collection)
        if payload.force
        else index_document(payload.doc_id, collection)
    )
    set_status(payload.doc_id, ContentStatus.INDEXED)
    log.info("indexed doc_id=%s vectors=%d", payload.doc_id, vectors)

    return [
        Emit(
            topic=TOPIC_INDEX_DONE,
            payload=IndexDone(
                doc_id=payload.doc_id, vectors=vectors, collection=collection
            ),
            type="index.done",
            key=payload.doc_id,
        ),
        _status_emit("embed", ContentStatus.INDEXED, payload.doc_id),
    ]


STAGES: dict[str, Stage] = {
    "fetch": Stage(
        name="fetch",
        topics=(TOPIC_INDEX_REQUESTS,),
        group_id=GROUP_FETCH,
        payload_model=IndexRequest,
        handler=handle_index_request,
        max_attempts=3,
    ),
    "chunk": Stage(
        name="chunk",
        topics=(TOPIC_CONTENT_FETCHED,),
        group_id=GROUP_CHUNK,
        payload_model=ContentFetched,
        handler=handle_content_fetched,
        max_attempts=2,
    ),
    "embed": Stage(
        name="embed",
        topics=(TOPIC_CHUNKS_READY,),
        group_id=GROUP_EMBED,
        payload_model=ChunksReady,
        handler=handle_chunks_ready,
        # Ретраить эмбеддинг дорого: одна попытка, дальше DLQ и разбор руками.
        max_attempts=1,
        consumer_overrides={"max.poll.interval.ms": EMBED_MAX_POLL_INTERVAL_MS},
    ),
}
