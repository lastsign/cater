from __future__ import annotations

from collections.abc import Iterable

from qdrant_client.http import models

from src.db_sync import get_db_sync
from src.embedder.config import EMBED_BATCH_SIZE, EMBED_PARALLEL, QDRANT_COLLECTION
from src.embedder.models import colbert_model, dense_model, passage_texts, sparse_model
from src.embedder.qdrant import (
    COLBERT_VECTOR_NAME,
    DENSE_VECTOR_NAME,
    SPARSE_VECTOR_NAME,
    client,
    ensure_collection,
)
from src.models.chunk import Chunk

UPSERT_BATCH = 256


def _load_chunks(doc_id: str) -> list[Chunk]:
    with get_db_sync() as db:
        return db.query(Chunk).filter(Chunk.content_id == doc_id).all()


def _build_points(chunks: list[Chunk]) -> Iterable[models.PointStruct]:
    texts = [c.text for c in chunks]

    dense_vecs = dense_model().embed(
        passage_texts(texts),
        batch_size=EMBED_BATCH_SIZE,
        parallel=EMBED_PARALLEL or None,
    )
    sparse_vecs = sparse_model().embed(texts, batch_size=EMBED_BATCH_SIZE)
    colbert_vecs = colbert_model().embed(
        texts, batch_size=EMBED_BATCH_SIZE, parallel=EMBED_PARALLEL or None
    )

    for chunk, dense, sparse, colbert in zip(
        chunks, dense_vecs, sparse_vecs, colbert_vecs, strict=True
    ):
        yield models.PointStruct(
            id=str(chunk.id),
            vector={
                DENSE_VECTOR_NAME: dense.tolist(),
                SPARSE_VECTOR_NAME: models.SparseVector(
                    indices=sparse.indices.tolist(),
                    values=sparse.values.tolist(),
                ),
                COLBERT_VECTOR_NAME: colbert.tolist(),
            },
            payload={
                "content_id": str(chunk.content_id),
                "chunk_index": chunk.chunk_index,
                "text": chunk.text,
            },
        )


def _chunked(
    it: Iterable[models.PointStruct], n: int
) -> Iterable[list[models.PointStruct]]:
    buf: list[models.PointStruct] = []
    for item in it:
        buf.append(item)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def index_document(doc_id: str, collection: str = QDRANT_COLLECTION) -> int:
    ensure_collection(collection)
    chunks = _load_chunks(doc_id)
    if not chunks:
        return 0

    qc = client()
    total = 0
    for batch in _chunked(_build_points(chunks), UPSERT_BATCH):
        qc.upsert(collection_name=collection, points=batch, wait=False)
        total += len(batch)
    return total


def _delete_document(doc_id: str, collection: str) -> None:
    client().delete(
        collection_name=collection,
        points_selector=models.FilterSelector(
            filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="content_id",
                        match=models.MatchValue(value=doc_id),
                    )
                ]
            )
        ),
        wait=True,
    )


def reindex_document(doc_id: str, collection: str = QDRANT_COLLECTION) -> int:
    ensure_collection(collection)
    _delete_document(doc_id, collection)
    return index_document(doc_id, collection=collection)
