from __future__ import annotations

import asyncio
from dataclasses import dataclass

from qdrant_client.http import models

from src.embedder.config import QDRANT_COLLECTION
from src.embedder.models import colbert_model, dense_model, query_text, sparse_model
from src.embedder.qdrant import (
    COLBERT_VECTOR_NAME,
    DENSE_VECTOR_NAME,
    SPARSE_VECTOR_NAME,
    aclient,
    client,
)


@dataclass(slots=True)
class Hit:
    id: str
    score: float
    text: str
    content_id: str
    chunk_index: int


def hybrid_search(
    query: str,
    limit: int = 10,
    prefetch_limit: int = 50,
    fusion_limit: int = 50,
    rerank: bool = True,
    collection: str = QDRANT_COLLECTION,
) -> list[Hit]:
    dense_vec = next(iter(dense_model().query_embed(query_text(query)))).tolist()
    sparse_vec = next(iter(sparse_model().query_embed(query)))

    dense_prefetch = models.Prefetch(
        query=dense_vec, using=DENSE_VECTOR_NAME, limit=prefetch_limit
    )
    sparse_prefetch = models.Prefetch(
        query=models.SparseVector(
            indices=sparse_vec.indices.tolist(),
            values=sparse_vec.values.tolist(),
        ),
        using=SPARSE_VECTOR_NAME,
        limit=prefetch_limit,
    )

    if rerank:
        colbert_vec = next(iter(colbert_model().query_embed(query))).tolist()
        result = client().query_points(
            collection_name=collection,
            prefetch=[
                models.Prefetch(
                    prefetch=[dense_prefetch, sparse_prefetch],
                    query=models.FusionQuery(fusion=models.Fusion.RRF),
                    limit=fusion_limit,
                ),
            ],
            query=colbert_vec,
            using=COLBERT_VECTOR_NAME,
            limit=limit,
            with_payload=True,
        )
    else:
        result = client().query_points(
            collection_name=collection,
            prefetch=[dense_prefetch, sparse_prefetch],
            query=models.FusionQuery(fusion=models.Fusion.RRF),
            limit=limit,
            with_payload=True,
        )

    return [
        Hit(
            id=str(p.id),
            score=p.score,
            text=p.payload["text"],
            content_id=p.payload["content_id"],
            chunk_index=p.payload["chunk_index"],
        )
        for p in result.points
    ]


async def ahybrid_search(
    query: str,
    *,
    limit: int = 10,
    prefetch_limit: int = 50,
    fusion_limit: int = 50,
    collection: str = QDRANT_COLLECTION,
    rerank: bool = True,
) -> list[Hit]:
    # TODO: serve models in k8s and gether vectors via gateway
    def _embed_all_models(query: str):
        dense_vec = next(iter(dense_model().query_embed(query_text(query)))).tolist()
        sparse_vec = next(iter(sparse_model().query_embed(query)))
        colbert_vec = next(iter(colbert_model().query_embed(query))).tolist()
        return (dense_vec, sparse_vec, colbert_vec)

    dense_vec, sparse_vec, colbert_vec = await asyncio.to_thread(
        _embed_all_models, query
    )

    dense_prefetch = models.Prefetch(
        query=dense_vec, using=DENSE_VECTOR_NAME, limit=prefetch_limit
    )
    sparse_prefetch = models.Prefetch(
        query=models.SparseVector(
            indices=sparse_vec.indices.tolist(),
            values=sparse_vec.values.tolist(),
        ),
        using=SPARSE_VECTOR_NAME,
        limit=prefetch_limit,
    )

    if rerank:
        result = await aclient().query_points(
            collection_name=collection,
            prefetch=[
                models.Prefetch(
                    prefetch=[dense_prefetch, sparse_prefetch],
                    query=models.FusionQuery(fusion=models.Fusion.RRF),
                    limit=fusion_limit,
                ),
            ],
            query=colbert_vec,
            using=COLBERT_VECTOR_NAME,
            limit=limit,
            with_payload=True,
        )
    else:
        result = await aclient().query_points(
            collection_name=collection,
            prefetch=[dense_prefetch, sparse_prefetch],
            query=models.FusionQuery(fusion=models.Fusion.RRF),
            limit=limit,
            with_payload=True,
        )

    return [
        Hit(
            id=str(p.id),
            score=p.score,
            text=p.payload["text"],
            content_id=p.payload["content_id"],
            chunk_index=p.payload["chunk_index"],
        )
        for p in result.points
    ]


async def main(query: str, rerank=False):
    res = await ahybrid_search(query, rerank=rerank)
    print(res)


if __name__ == "__main__":
    # res = hybrid_search("nvidia", rerank=False)
    # print(res)

    asyncio.run(main("nvidia", True))
