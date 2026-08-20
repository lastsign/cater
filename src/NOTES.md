# Celery RAG pipeline — notes and plan

This document describes the current state of `src/`, what is broken, what still has to be
written, and the pitfalls of the fetch → chunk → embed → upsert-into-Qdrant architecture.

Read it top to bottom. The "Pitfalls" section is a reference — come back to it as the load
grows.

---

## 0. TL;DR startup order (once everything is finished)

From the repo root `/home/ptycho/cater`:

```bash
# 1. Infrastructure
docker compose -f src/docker-compose.yml up -d
# (once we add Qdrant — it goes here too)

# 2. Apply migrations / create the tables
python -m src.scripts.create_tables   # to be replaced by alembic later

# 3. Celery workers (at least 3 queues)
celery -A src.celery_service.tasks worker -Q io -c 20 --pool=gevent --loglevel=info
celery -A src.celery_service.tasks worker -Q embed -c 4 --loglevel=info
celery -A src.celery_service.tasks worker -Q qdrant -c 2 --loglevel=info

# 4. FastAPI
uvicorn src.main:app --reload
```

---

## 1. What already exists in `src/` (audit)

### Works / fine:
- `db.py` — async SQLAlchemy engine + `SessionLocal` + `get_db()` for FastAPI. OK.
- `db_sync.py` — sync engine on `psycopg`. **Required specifically for Celery tasks** (we do not use an async session inside tasks — see pitfall #8).
- `models/base.py` — `Base`, `UUIDMixin`, `TimestampMixin`. OK.
- `models/content.py` — `Content` + `ContentText` (the body in its own table — good for keeping large texts apart from metadata).
- `models/chunk.py` — `Chunk` with `content_id`, `chunk_index`, `text`, `text_hash`, `token_count`. Uniqueness on `(content_id, chunk_index)`. Good.
- `docker-compose.yml` — Redis + Postgres, `POSTGRES_*` fixed. OK.
- `celery_service/celery_conn.py` — `app` with the broker/backend on Redis. OK.
- `celery_service/producer.py` — a separate producer, the side effect is out of `tasks.py`. OK.

### Stubs (empty files):
- `storage.py` — 0 lines.
- `utils.py` — 0 lines.

### Not created at all:
- `embeddings.py` — an `embed(text)` / `embed_many(texts)` function.
- `qdrant_client.py` — the Qdrant client + collection creation.
- `scripts/create_tables.py` — DB bootstrap (or alembic).
- Qdrant in `docker-compose.yml`.
- A field to store the vector in `Chunk` (or the vector lives only in Qdrant — see below).

### Broken right now (`celery_service/tasks.py`):

| Line | Problem | Fix |
|---|---|---|
| 14 | `uuid.uuid()` | `uuid.uuid4()` |
| 19 | `@app.tast` — typo | `@app.task` |
| 36 | `chunk.page_content` — an expression going nowhere | `text = chunk.page_content` |
| 38 | `save_chunk(cid, c)` — there is no variable `c` | `save_chunk(cid, text)` |
| 50 | `save_vector(...)` — not imported | Either import it, or do not store in Postgres at all and carry the vector onward as a payload (see §3, "the architectural decision about storing vectors") |
| 56 | `load_vectors` — not imported | Implement it in `storage.py` or drop the scheme (see below) |
| 59 | typo `paylaod` | `payload` |
| 71 | `raise self.replace(workflow)` | `self.replace()` raises on its own, no `raise` needed. Just `return self.replace(workflow)` or `self.replace(workflow)` without `raise`. |
| 74 | `async def index_url_pipeline` | Must not be `async`. `.apply_async()` is not a coroutine, it is just a celery method that synchronously returns an `AsyncResult`. |
| 78 | `dispatch_embeddings(collection)` — a call instead of a signature | `dispatch_embeddings.s(collection)` |
| — | The tasks have no queues | Add `queue=...` (see pitfall #4) |

### Broken in `main.py`:
- Lines 28, 33: `index_url_pipeline`, `AsyncResult`, `celery_app` are used — nothing is imported.
- Line 28: `await index_url_pipeline(...)` — the function must not be async, and `await` is not needed here.

### Broken in `conn.py`:
- The file runs `r.set / r.get` at module level on import. That is a test script, not a module. Either wrap it in `if __name__ == "__main__":` or delete it — you already have Redis through Celery, a separate connection is not needed for now.

---

## 2. Pipeline architecture (target)

```
POST /index {url}
       │
       ▼
   fetch_url(url)
       │  returns: content_id (UUID of the Content row)
       ▼
   split_into_chunks(content_id)
       │  returns: [chunk_id_1, chunk_id_2, ...]
       ▼
   dispatch_embeddings(chunk_ids)        ← dispatcher task
       │
       │  self.replace(chord(...))
       ▼
   ┌─ embed_batch([cid, ...])  ┐
   ├─ embed_batch([cid, ...])  ├─ group (in parallel)
   └─ embed_batch([cid, ...])  ┘
       │
       │  results are collected → chord callback
       ▼
   upsert_to_qdrant(list[list[dict]])
       │
       ▼
   update Content.status = "indexed"
```

What lives where:
- **Postgres**: metadata (`Content`), the full text (`ContentText`), the chunks (`Chunk` — id, text, text_hash). The source of truth.
- **Qdrant**: only vectors + a payload with `chunk_id` and optionally a piece of the text (for a fast preview in search).
- **Redis (Celery broker)**: only messages saying "run task X with argument chunk_id=Y". **No texts and no vectors through the broker**.

That is "do not drag big data through the broker" (pitfall #1) put into practice.

---

## 3. What has to be created / written

### 3.1 `src/storage.py`

A thin wrapper over Postgres for the tasks. Sync sessions (for Celery), not async.

```python
from sqlalchemy import select
from src.db_sync import SessionLocalSync
from src.models.content import Content, ContentText
from src.models.chunk import Chunk

def create_content(source_url: str, source_type: str = "url") -> str:
    with SessionLocalSync() as s, s.begin():
        c = Content(source_url=source_url, source_type=source_type, status="fetching")
        s.add(c)
        s.flush()
        return str(c.id)

def save_text(content_id: str, text: str, content_hash: bytes) -> None:
    with SessionLocalSync() as s, s.begin():
        s.add(ContentText(content_id=content_id, text=text))
        s.execute(
            update(Content)
            .where(Content.id == content_id)
            .values(content_hash=content_hash, status="fetched")
        )

def load_text(content_id: str) -> str:
    with SessionLocalSync() as s:
        return s.scalar(
            select(ContentText.text).where(ContentText.content_id == content_id)
        )

def save_chunks_bulk(content_id: str, chunks: list[tuple[int, str, bytes]]) -> list[str]:
    """chunks: [(chunk_index, text, text_hash), ...] -> [chunk_id, ...]"""
    with SessionLocalSync() as s, s.begin():
        objs = [Chunk(content_id=content_id, chunk_index=i, text=t, text_hash=h)
                for i, t, h in chunks]
        s.add_all(objs)
        s.flush()
        return [str(o.id) for o in objs]

def load_chunks(chunk_ids: list[str]) -> list[tuple[str, str]]:
    """-> [(chunk_id, text), ...] in the same order as chunk_ids."""
    with SessionLocalSync() as s:
        rows = s.execute(
            select(Chunk.id, Chunk.text).where(Chunk.id.in_(chunk_ids))
        ).all()
        by_id = {str(r.id): r.text for r in rows}
        return [(cid, by_id[cid]) for cid in chunk_ids]
```

Principles:
- **Bulk insert** the chunks in a single transaction — otherwise long documents mean N round-trips to the DB.
- `with session.begin()` — one transaction, auto-commit on exit.
- Return strings (UUID → str), because Celery serializes arguments into JSON.

### 3.2 `src/embeddings.py`

```python
import os
from openai import OpenAI

_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
EMBED_MODEL = "text-embedding-3-small"
EMBED_DIM = 1536

def embed_many(texts: list[str]) -> list[list[float]]:
    resp = _client.embeddings.create(model=EMBED_MODEL, input=texts)
    return [d.embedding for d in resp.data]
```

A batch request is one HTTP round-trip per N chunks. Used in the `embed_batch` task.

### 3.3 `src/qdrant_client.py`

```python
import os
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

qdrant = QdrantClient(
    host=os.getenv("QDRANT_HOST", "localhost"),
    port=int(os.getenv("QDRANT_PORT", "6333")),
)

def ensure_collection(name: str, dim: int) -> None:
    if not qdrant.collection_exists(name):
        qdrant.create_collection(
            collection_name=name,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
        )
```

### 3.4 `docker-compose.yml` — add Qdrant

```yaml
  qdrant:
    image: qdrant/qdrant:latest
    ports:
      - "6333:6333"
      - "6334:6334"
    volumes:
      - qdrant_data:/qdrant/storage
    restart: always

volumes:
  redis_data:
  pg_data:
  qdrant_data:
```

### 3.5 `src/scripts/create_tables.py`

```python
from src.db_sync import engine_sync
from src.models.base import Base
import src.models.content  # noqa: F401 — needed so the models get registered
import src.models.chunk    # noqa: F401

if __name__ == "__main__":
    Base.metadata.create_all(engine_sync)
    print("tables created")
```

(To be replaced by alembic later — this is just a bootstrap.)

### 3.6 Rewrite `celery_service/tasks.py` (target version)

```python
import hashlib
import uuid
import httpx
from celery import chord, group, chain
from src.celery_service.celery_conn import app
from src.storage import (
    create_content, save_text, load_text,
    save_chunks_bulk, load_chunks,
)
from src.embeddings import embed_many, EMBED_DIM
from src.qdrant_client import qdrant, ensure_collection

COLLECTION = "hugging-face-collection"
EMBED_BATCH_SIZE = 32

def _sha256(s: str) -> bytes:
    return hashlib.sha256(s.encode("utf-8")).digest()

def _chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


@app.task(name="fetch_url", queue="io", autoretry_for=(httpx.HTTPError,),
          retry_backoff=True, max_retries=3)
def fetch_url(url: str) -> str:
    content_id = create_content(source_url=url)
    text = httpx.get(url, timeout=30).text
    save_text(content_id, text, content_hash=_sha256(text))
    return content_id


@app.task(name="split_into_chunks", queue="io")
def split_into_chunks(content_id: str) -> list[str]:
    from langchain_text_splitters import RecursiveCharacterTextSplitter
    text = load_text(content_id)
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000, chunk_overlap=200, length_function=len, add_start_index=True,
    )
    docs = splitter.create_documents([text])
    rows = [(i, d.page_content, _sha256(d.page_content)) for i, d in enumerate(docs)]
    return save_chunks_bulk(content_id, rows)


@app.task(name="embed_batch", queue="embed")
def embed_batch(chunk_ids: list[str]) -> list[dict]:
    pairs = load_chunks(chunk_ids)              # [(id, text), ...]
    vectors = embed_many([t for _, t in pairs]) # batch API call
    return [
        {"id": cid, "vector": v, "text": t}
        for (cid, t), v in zip(pairs, vectors)
    ]


@app.task(name="upsert_to_qdrant", queue="qdrant")
def upsert_to_qdrant(batches: list[list[dict]], collection: str = COLLECTION) -> int:
    ensure_collection(collection, EMBED_DIM)
    points = [
        {"id": item["id"], "vector": item["vector"],
         "payload": {"chunk_id": item["id"], "text": item["text"]}}
        for batch in batches for item in batch
    ]
    qdrant.upsert(collection_name=collection, points=points)
    return len(points)


@app.task(name="dispatch_embeddings", bind=True, queue="io")
def dispatch_embeddings(self, chunk_ids: list[str], collection: str = COLLECTION):
    workflow = chord(
        group(embed_batch.s(list(b)) for b in _chunked(chunk_ids, EMBED_BATCH_SIZE)),
        upsert_to_qdrant.s(collection),
    )
    return self.replace(workflow)   # without raise


def index_url_pipeline(url: str, collection: str = COLLECTION):
    return chain(
        fetch_url.s(url),
        split_into_chunks.s(),
        dispatch_embeddings.s(collection),
    ).apply_async()
```

### 3.7 Fix `main.py`

```python
from fastapi import FastAPI
from celery.result import AsyncResult
from src.celery_service.celery_conn import app as celery_app
from src.celery_service.tasks import index_url_pipeline

app = FastAPI()

@app.post("/index")
def index(url: str):
    return {"task_id": index_url_pipeline(url).id}

@app.get("/index/{task_id}")
def status(task_id: str):
    res = AsyncResult(task_id, app=celery_app)
    if not res.ready():
        return {"state": res.state}
    if res.failed():
        return {"state": "FAILURE", "error": str(res.result)}
    return {"state": "SUCCESS", "points_upserted": res.result}
```

Note: **plain `def`, not `async def`**. Celery has a synchronous API.

---

## 4. "What is left to do" checklist

- [ ] Fix the typos in `tasks.py` (see the table in §1).
- [ ] Create `storage.py` (see §3.1).
- [ ] Create `embeddings.py` (see §3.2). Put `OPENAI_API_KEY` into `.env`.
- [ ] Create `qdrant_client.py` (see §3.3).
- [ ] Add Qdrant to `docker-compose.yml` (see §3.4).
- [ ] Create `scripts/create_tables.py` (see §3.5).
- [ ] Rewrite `tasks.py` completely (§3.6).
- [ ] Fix `main.py` (§3.7).
- [ ] Clean up `conn.py` (either delete it or wrap it in `__main__`).
- [ ] Run end-to-end: `POST /index` → `GET /index/{id}` → a query against Qdrant.
- [ ] Only after that works: introduce the queues (`-Q io / embed / qdrant`) and batching.

**Order matters**: first a linear pipeline without optimizations, then chord/batching/queues.
Do not do it all at once — debugging becomes impossible.

---

## 5. Pitfalls (extended reference)

### 5.1 Do not drag big data through the broker

**What not to do:** pass into `.delay(...)` or return from a task: full page texts, PDFs, arrays of thousands of chunks, embedding vectors.

**Why:** every task argument is serialized into JSON and written into Redis as a message. The result is written as the key `celery-task-meta-<id>` with a 24-hour TTL. Returning 5000 vectors of 1536 floats is ~60 MB in Redis for a single task. Redis becomes the bottleneck faster than you expect.

**What to do:** keep the body in storage (Postgres/S3/disk), push only `content_id` / `chunk_id` through the broker. Our architecture already accounts for this: the chunks live in Postgres, only UUIDs fly through the queue.

### 5.2 Task idempotency

**Fact:** any task may run **twice**. The worker dies after finishing the work but before sending the `ack` to the broker — the broker considers the task unfinished and hands it to another worker.

**What to do:**
- Use deterministic ids (for instance sha256 of the chunk content) — then a repeated `qdrant.upsert` with the same id overwrites the point instead of creating a duplicate.
- Do not perform side effects without an idempotency key (payments, sending email — only with a dedup key).
- In Postgres use `INSERT ... ON CONFLICT DO NOTHING / DO UPDATE`.

### 5.3 `acks_late` and losing tasks

**By default** Celery sends the ack to the broker **before** execution. If the worker dies mid-execution, the task is lost.

**When losing it is not acceptable:**
```python
@app.task(acks_late=True, reject_on_worker_lost=True)
def critical(...): ...
```

You pay for it with a higher chance of double execution (see §5.2 — idempotency is mandatory).

For our pipeline: `fetch_url` is idempotent (it can be re-run), `upsert_to_qdrant` is too (a Qdrant upsert by id). It is safe to enable `acks_late=True` globally.

### 5.4 Different queues for different stages

**Problem:** with everything in one queue, a slow `embed` (an HTTP call to OpenAI, ~500 ms) fills the pool and the fast `fetch` tasks queue up behind it.

**Solution:**
```python
@app.task(queue="io")    # network, many in parallel
@app.task(queue="embed") # limited by the API rate limit
@app.task(queue="qdrant") # writes into the vector DB
```

Run **separate worker processes for separate queues**:
```bash
celery -A ... worker -Q io     -c 50 --pool=gevent
celery -A ... worker -Q embed  -c 4  --pool=prefork
celery -A ... worker -Q qdrant -c 2  --pool=prefork
```

`--pool=gevent` for IO-bound work (network) — it allows a thousand "threads" per process.
`--pool=prefork` (the default) for CPU-bound work.

### 5.5 `visibility_timeout` of the Redis broker

**The default is 1 hour.** If a task runs longer, the Redis broker considers the worker dead and **hands the task to another worker**. You get parallel double execution without any crash at all.

If you expect long tasks (a big PDF, a heavy embedding) — raise it:
```python
app.conf.broker_transport_options = {"visibility_timeout": 3600 * 12}  # 12 hours
```

The alternative (and the better one) is to **split the work more finely**, so that no single task runs longer than a few minutes.

### 5.6 Chord group size

A chord on Redis is implemented by polling a counter of finished tasks in the group. For a group of 100 tasks that is fine. For a group of 10,000 tasks Redis starts to suffer and the callback may be delayed or break.

**In our pipeline:** with `EMBED_BATCH_SIZE = 32` a document of 10,000 chunks produces a chord of ~313 tasks — fine. If documents of 100,000 chunks show up, raise the batch to 128 or split into sub-pipelines.

### 5.7 Imports at the top level of the task module

Remember: on startup the worker imports `tasks.py`. Everything at module level runs. Therefore:
- ❌ `add.delay(4, 4)` at module level (it would send a task on every worker start).
- ❌ `qdrant.upsert(...)` at module level.
- ❌ Heavy imports such as `import torch` — better inside the task (`def`), so they are not loaded during autodiscovery.

In our `tasks.py` (§3.6) `langchain_text_splitters` is imported inside the task for exactly this reason.

### 5.8 Async and Celery are incompatible (important with a FastAPI habit)

Celery 5.x is a **synchronous framework**. Inside tasks you cannot use SQLAlchemy's `AsyncSession` directly — you would need either `asyncio.run(...)` (slow, a new event loop per task) or a sync session kept alongside. That is exactly why the project has **both `db.py` (async for FastAPI) and `db_sync.py` (sync for Celery)**.

In FastAPI handlers that work with Celery (`.delay`, `AsyncResult.ready()`) use **plain `def` handlers**. Async gives no benefit at all (the Celery API is synchronous) and there is a risk of accidentally blocking the event loop.

### 5.9 PENDING ≠ "the task exists"

`AsyncResult(random_uuid).state` returns `PENDING`. Celery does not know whether a task with that id exists — it simply says "I see no result". That means polling by an id received from the frontend can return PENDING forever if you made a typo.

To tell "the task is queued" from "there is no such task":
```python
app.conf.task_track_started = True   # a STARTED state appears
```
And/or keep the created task_ids in your own table (a `Job` with `task_id`, `status`, `created_at`) — then you have a source of truth.

### 5.10 Polling vs WebSocket/SSE

`GET /index/{task_id}` every 2 seconds from the frontend works, but it is noisy. Production options:
- **SSE** (Server-Sent Events) — a simple one-way stream of events from the server.
- **WebSocket** + Redis pub/sub: at the end the task does `redis.publish("job:{id}", "done")`, FastAPI is subscribed and pushes to the client.
- The Celery signal `task_postrun` — a global hook on the completion of any task.

For a prototype polling is fine. Change it when it becomes the bottleneck.

---

## 6. Next steps (once the pipeline works)

- Alembic instead of `create_all`.
- A `Job` table to track the indexing status (see §5.9).
- Retries on embedding (`autoretry_for=(openai.RateLimitError,)`, `retry_backoff=True`).
- Metrics: a Prometheus exporter for Celery (`celery-exporter`), Flower for debugging.
- Testing tasks without a worker: `task.apply(args=[...])` runs them synchronously in the current process.
- PDFs: a new step `extract_text(file_id) → content_id` before `split_into_chunks`. Files go to S3/MinIO (not into Postgres).
- Search: a separate endpoint `POST /search {query}` → embed the query → `qdrant.search` → return the `chunk_id`s → pull the texts from Postgres.
