import os

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY") or None
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "hugging-face-collection")

DENSE_MODEL = os.getenv("DENSE_MODEL", "intfloat/multilingual-e5-large")
DENSE_DIM = int(os.getenv("DENSE_DIM", "1024"))
SPARSE_MODEL = os.getenv("SPARSE_MODEL", "Qdrant/bm25")
COLBERT_MODEL = os.getenv("COLBERT_MODEL", "colbert-ir/colbertv2.0")
COLBERT_DIM = int(os.getenv("COLBERT_DIM", "128"))

EMBED_BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "32"))
EMBED_PARALLEL = int(os.getenv("EMBED_PARALLEL", "0"))  # 0 = use CUDA single-process
USE_GPU = os.getenv("EMBED_USE_GPU", "1") == "1"
