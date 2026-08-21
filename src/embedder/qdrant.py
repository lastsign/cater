from functools import lru_cache

from qdrant_client import AsyncQdrantClient, QdrantClient
from qdrant_client.http import models

from src.embedder.config import (
    COLBERT_DIM,
    DENSE_DIM,
    QDRANT_API_KEY,
    QDRANT_COLLECTION,
    QDRANT_URL,
)

DENSE_VECTOR_NAME = "dense"
SPARSE_VECTOR_NAME = "bm25"
COLBERT_VECTOR_NAME = "colbert"


@lru_cache(maxsize=1)
def client() -> QdrantClient:
    return QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY, prefer_grpc=True)


@lru_cache(maxsize=1)
def aclient() -> AsyncQdrantClient:
    return AsyncQdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY, prefer_grpc=True)


def ensure_collection(collection: str = QDRANT_COLLECTION) -> None:
    c = client()
    if c.collection_exists(collection):
        return
    c.create_collection(
        collection_name=collection,
        vectors_config={
            DENSE_VECTOR_NAME: models.VectorParams(
                size=DENSE_DIM,
                distance=models.Distance.COSINE,
            ),
            # ColBERT is reranker-only: store vectors, skip HNSW graph (m=0).
            COLBERT_VECTOR_NAME: models.VectorParams(
                size=COLBERT_DIM,
                distance=models.Distance.COSINE,
                multivector_config=models.MultiVectorConfig(
                    comparator=models.MultiVectorComparator.MAX_SIM,
                ),
                hnsw_config=models.HnswConfigDiff(m=0),
            ),
        },
        sparse_vectors_config={
            SPARSE_VECTOR_NAME: models.SparseVectorParams(
                modifier=models.Modifier.IDF,
            ),
        },
    )
    c.create_payload_index(collection, "content_id", models.PayloadSchemaType.KEYWORD)
