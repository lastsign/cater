# Celery RAG-пайплайн — заметки и план

Документ описывает текущее состояние `src/`, что сломано, что нужно дописать,
и подводные камни архитектуры fetch → chunk → embed → upsert в Qdrant.

Читать сверху вниз. Раздел «Подводные камни» — справочный, к нему возвращаться
по мере роста нагрузки.

---

## 0. TL;DR порядок запуска (когда всё доделаем)

Из корня репо `/home/ptycho/cater`:

```bash
# 1. Инфра
docker compose -f src/docker-compose.yml up -d
# (когда добавим Qdrant — он тоже сюда)

# 2. Применить миграции / создать таблицы
python -m src.scripts.create_tables   # потом заменим на alembic

# 3. Celery worker (минимум 3 очереди)
celery -A src.celery_service.tasks worker -Q io -c 20 --pool=gevent --loglevel=info
celery -A src.celery_service.tasks worker -Q embed -c 4 --loglevel=info
celery -A src.celery_service.tasks worker -Q qdrant -c 2 --loglevel=info

# 4. FastAPI
uvicorn src.main:app --reload
```

---

## 1. Что уже есть в `src/` (аудит)

### Работает / норм:
- `db.py` — async SQLAlchemy engine + `SessionLocal` + `get_db()` для FastAPI. Ок.
- `db_sync.py` — sync engine на `psycopg`. **Нужен именно для Celery-задач** (внутри тасков не используем async-сессию — см. подводный камень №8).
- `models/base.py` — `Base`, `UUIDMixin`, `TimestampMixin`. Ок.
- `models/content.py` — `Content` + `ContentText` (тело отдельной таблицей — хорошо для хранения больших текстов отдельно от метаданных).
- `models/chunk.py` — `Chunk` с `content_id`, `chunk_index`, `text`, `text_hash`, `token_count`. Уникальность `(content_id, chunk_index)`. Хорошо.
- `docker-compose.yml` — Redis + Postgres, `POSTGRES_*` починили. Ок.
- `celery_service/celery_conn.py` — `app` с brokerом/backendом на Redis. Ок.
- `celery_service/producer.py` — отдельный продюсер, side-effect убран из `tasks.py`. Ок.

### Заглушки (пустые файлы):
- `storage.py` — 0 строк.
- `utils.py` — 0 строк.

### Не создано вообще:
- `embeddings.py` — функция `embed(text)` / `embed_many(texts)`.
- `qdrant_client.py` — клиент Qdrant + создание коллекции.
- `scripts/create_tables.py` — bootstrap БД (или alembic).
- Qdrant в `docker-compose.yml`.
- Поле для хранения вектора в `Chunk` (или вектор живёт только в Qdrant — см. ниже).

### Сломано прямо сейчас (`celery_service/tasks.py`):

| Строка | Проблема | Фикс |
|---|---|---|
| 14 | `uuid.uuid()` | `uuid.uuid4()` |
| 19 | `@app.tast` — опечатка | `@app.task` |
| 36 | `chunk.page_content` — выражение в пустоту | `text = chunk.page_content` |
| 38 | `save_chunk(cid, c)` — нет переменной `c` | `save_chunk(cid, text)` |
| 50 | `save_vector(...)` — не импортирована | Либо импортировать, либо вообще не сохранять в Postgres, а тащить вектор дальше как payload (см. п. 3 «архитектурное решение про хранение векторов») |
| 56 | `load_vectors` — не импортирована | Реализовать в `storage.py` или отказаться от схемы (см. ниже) |
| 59 | опечатка `paylaod` | `payload` |
| 71 | `raise self.replace(workflow)` | `self.replace()` сам бросает исключение, `raise` не нужен. Просто `return self.replace(workflow)` или `self.replace(workflow)` без `raise`. |
| 74 | `async def index_url_pipeline` | Не должно быть `async`. `.apply_async()` — это не coroutine, это просто метод celery, который синхронно возвращает `AsyncResult`. |
| 78 | `dispatch_embeddings(collection)` — вызов вместо signature | `dispatch_embeddings.s(collection)` |
| — | Нет очередей у задач | Добавить `queue=...` (см. подводный камень №4) |

### Сломано в `main.py`:
- Строки 28, 33: используются `index_url_pipeline`, `AsyncResult`, `celery_app` — ничего не импортировано.
- Строка 28: `await index_url_pipeline(...)` — функция не должна быть async, и `await` тут не нужен.

### Сломано в `conn.py`:
- Файл выполняет `r.set / r.get` на верхнем уровне при импорте. Это тестовый скрипт, не модуль. Либо обернуть в `if __name__ == "__main__":`, либо удалить — у тебя уже есть Redis через Celery, отдельный коннект пока не нужен.

---

## 2. Архитектура пайплайна (целевая)

```
POST /index {url}
       │
       ▼
   fetch_url(url)
       │  возвращает: content_id (UUID Content-записи)
       ▼
   split_into_chunks(content_id)
       │  возвращает: [chunk_id_1, chunk_id_2, ...]
       ▼
   dispatch_embeddings(chunk_ids)        ← задача-диспетчер
       │
       │  self.replace(chord(...))
       ▼
   ┌─ embed_batch([cid, ...])  ┐
   ├─ embed_batch([cid, ...])  ├─ group (параллельно)
   └─ embed_batch([cid, ...])  ┘
       │
       │  результаты собираются → callback chord
       ▼
   upsert_to_qdrant(list[list[dict]])
       │
       ▼
   обновить Content.status = "indexed"
```

Что лежит где:
- **Postgres**: метаданные (`Content`), полный текст (`ContentText`), чанки (`Chunk` — id, text, text_hash). Источник правды.
- **Qdrant**: только векторы + payload с `chunk_id` и опционально кусок текста (для быстрого превью при поиске).
- **Redis (Celery broker)**: только сообщения «выполни задачу X с аргументом chunk_id=Y». **Никакого текста и векторов через broker**.

Это и есть «не таскай большие данные через broker» (подводный камень №1) на практике.

---

## 3. Что нужно создать / дописать

### 3.1 `src/storage.py`

Тонкая обёртка над Postgres для задач. Sync-сессии (для Celery), не async.

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
    """-> [(chunk_id, text), ...] в том же порядке, что и chunk_ids."""
    with SessionLocalSync() as s:
        rows = s.execute(
            select(Chunk.id, Chunk.text).where(Chunk.id.in_(chunk_ids))
        ).all()
        by_id = {str(r.id): r.text for r in rows}
        return [(cid, by_id[cid]) for cid in chunk_ids]
```

Принципы:
- **Bulk-вставка** чанков одной транзакцией — иначе на длинных документах будет N round-trip к БД.
- `with session.begin()` — единая транзакция, авто-commit на выходе.
- Возвращаем строки (UUID → str), потому что Celery сериализует аргументы в JSON.

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

Батч-запрос — один HTTP round-trip на N чанков. Используется в `embed_batch`-задаче.

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

### 3.4 `docker-compose.yml` — добавить Qdrant

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
import src.models.content  # noqa: F401 — нужно, чтобы модели зарегистрировались
import src.models.chunk    # noqa: F401

if __name__ == "__main__":
    Base.metadata.create_all(engine_sync)
    print("tables created")
```

(Потом заменим на alembic — пока bootstrap.)

### 3.6 Переписать `celery_service/tasks.py` (целевая версия)

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
    return self.replace(workflow)   # без raise


def index_url_pipeline(url: str, collection: str = COLLECTION):
    return chain(
        fetch_url.s(url),
        split_into_chunks.s(),
        dispatch_embeddings.s(collection),
    ).apply_async()
```

### 3.7 Поправить `main.py`

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

Обрати внимание: **обычные `def`, не `async def`**. Celery — синхронное API.

---

## 4. Чек-лист «что доделать»

- [ ] Починить опечатки в `tasks.py` (см. таблицу в §1).
- [ ] Создать `storage.py` (см. §3.1).
- [ ] Создать `embeddings.py` (см. §3.2). Положить `OPENAI_API_KEY` в `.env`.
- [ ] Создать `qdrant_client.py` (см. §3.3).
- [ ] Добавить Qdrant в `docker-compose.yml` (см. §3.4).
- [ ] Создать `scripts/create_tables.py` (см. §3.5).
- [ ] Переписать `tasks.py` целиком (§3.6).
- [ ] Починить `main.py` (§3.7).
- [ ] Очистить `conn.py` (либо удалить, либо обернуть в `__main__`).
- [ ] Прогнать end-to-end: `POST /index` → `GET /index/{id}` → запрос в Qdrant.
- [ ] Только после того как работает: ввести очереди (`-Q io / embed / qdrant`) и батчинг.

**Порядок важен**: сначала линейный пайплайн без оптимизаций, потом chord/батчинг/очереди. Не делай всё сразу — отлаживать невозможно.

---

## 5. Подводные камни (расширенный справочник)

### 5.1 Не таскай большие данные через broker

**Что не делать:** передавать в `.delay(...)` или возвращать из задачи: полные тексты страниц, PDF, массивы из тысяч чанков, embedding-векторы.

**Почему:** каждый аргумент задачи сериализуется в JSON и пишется в Redis как сообщение. Результат — пишется как ключ `celery-task-meta-<id>` с TTL 24 часа. Если возвращаешь 5000 векторов по 1536 float — это ~60 МБ в Redis на одну задачу. Redis станет ботлнеком быстрее, чем ты ожидаешь.

**Что делать:** хранить тело в storage (Postgres/S3/диск), через broker гонять только `content_id` / `chunk_id`. В нашей архитектуре это уже учтено: чанки лежат в Postgres, через очередь летят только UUID.

### 5.2 Идемпотентность задач

**Факт:** любая задача может выполниться **дважды**. Worker умирает после выполнения, но до отправки `ack` в broker — broker считает её невыполненной и отдаёт другому worker'у.

**Что делать:**
- Использовать детерминированные id (например, sha256 от контента чанка) — тогда повторный `qdrant.upsert` с тем же id перезапишет точку, а не создаст дубль.
- Не делать сайд-эффекты без идемпотентного ключа (платежи, отправка email — только с dedup key).
- В Postgres использовать `INSERT ... ON CONFLICT DO NOTHING / DO UPDATE`.

### 5.3 `acks_late` и потеря задач

**По умолчанию** Celery шлёт ack в broker **до** выполнения. Если worker упадёт посреди выполнения — задача потеряна.

**Когда важно не потерять:**
```python
@app.task(acks_late=True, reject_on_worker_lost=True)
def critical(...): ...
```

Платишь за это повышенной вероятностью двойного выполнения (см. §5.2 — идемпотентность обязательна).

Для нашего пайплайна: `fetch_url` идемпотентен (можно перезапустить), `upsert_to_qdrant` тоже (Qdrant upsert по id). Безопасно включить `acks_late=True` глобально.

### 5.4 Разные очереди для разных стадий

**Проблема:** если всё в одной очереди, медленный `embed` (HTTP к OpenAI ~500мс) забьёт пул, и быстрые `fetch` будут стоять в очереди.

**Решение:**
```python
@app.task(queue="io")    # сеть, много параллельных
@app.task(queue="embed") # ограничен API rate limit
@app.task(queue="qdrant") # запись в векторную БД
```

Запускать **разные worker-процессы на разные очереди**:
```bash
celery -A ... worker -Q io     -c 50 --pool=gevent
celery -A ... worker -Q embed  -c 4  --pool=prefork
celery -A ... worker -Q qdrant -c 2  --pool=prefork
```

`--pool=gevent` для IO-bound (сеть) — позволяет тысячу «потоков» на процесс.
`--pool=prefork` (дефолт) для CPU-bound.

### 5.5 `visibility_timeout` Redis broker

**Дефолт — 1 час.** Если задача выполняется дольше → Redis-broker считает worker мёртвым и **отдаёт задачу другому worker'у**. Получаешь параллельное двойное выполнение без всякого падения.

Если ожидаешь длинные задачи (большой PDF, тяжёлый embedding) — поднять:
```python
app.conf.broker_transport_options = {"visibility_timeout": 3600 * 12}  # 12 часов
```

Альтернатива (лучше) — **дробить работу мельче**, чтобы ни одна задача не выполнялась дольше нескольких минут.

### 5.6 Размер chord-группы

Chord на Redis реализован через polling счётчика выполненных задач из группы. Для группы из 100 задач — норм. Для группы из 10 000 задач — Redis начинает страдать, callback может задерживаться или ломаться.

**В нашем пайплайне:** при `EMBED_BATCH_SIZE = 32` документ на 10 000 чанков даёт chord из ~313 задач — норм. Если будут документы по 100 000 чанков — батч поднимаем до 128 или дробим на под-пайплайны.

### 5.7 Imports на верхнем уровне модуля задач

Помни: воркер при старте импортирует `tasks.py`. Всё, что на верхнем уровне модуля, выполнится. Поэтому:
- ❌ `add.delay(4, 4)` на модуле (будет слать задачу при каждом старте воркера).
- ❌ `qdrant.upsert(...)` на модуле.
- ❌ Тяжёлые импорты типа `import torch` — лучше внутри задачи (`def`), чтобы не грузить при автодискавери.

В нашем `tasks.py` (§3.6) `langchain_text_splitters` импортируется внутри задачи — именно поэтому.

### 5.8 Async и Celery несовместимы (важно для FastAPI-привычки)

Celery 5.x — **синхронный фреймворк**. Внутри задач нельзя использовать `AsyncSession` SQLAlchemy напрямую — придётся либо `asyncio.run(...)` (медленно, новый event loop на каждую задачу), либо держать sync-сессию параллельно. Именно поэтому в проекте есть **и `db.py` (async для FastAPI), и `db_sync.py` (sync для Celery)**.

В FastAPI-хендлерах при работе с Celery (`.delay`, `AsyncResult.ready()`) — используй **обычные `def`-хендлеры**. Async не даст никакого выигрыша (Celery API синхронное), и есть риск случайно заблокировать event loop.

### 5.9 PENDING ≠ «задача существует»

`AsyncResult(random_uuid).state` вернёт `PENDING`. Celery не знает, существует ли задача с таким id — он просто говорит «не вижу результата». Это значит, что polling по id, полученному с фронта, может вечно возвращать PENDING, если ты опечатался.

Чтобы различать «задача в очереди» vs «такой задачи нет»:
```python
app.conf.task_track_started = True   # появится state STARTED
```
И/или храни созданные task_id в собственной таблице (`Job` с `task_id`, `status`, `created_at`) — тогда есть источник правды.

### 5.10 Polling vs WebSocket/SSE

`GET /index/{task_id}` каждые 2 секунды с фронта — работает, но шумно. Production-варианты:
- **SSE** (Server-Sent Events) — простой однонаправленный поток событий от сервера.
- **WebSocket** + Redis pub/sub: задача в конце делает `redis.publish("job:{id}", "done")`, FastAPI подписан и пушит клиенту.
- Celery signal `task_postrun` — глобальный хук на завершение любой задачи.

Для прототипа polling нормален. Менять, когда станет узким местом.

---

## 6. Дальнейшее (после того как пайплайн заработает)

- Alembic вместо `create_all`.
- `Job`-таблица для отслеживания статуса индексации (см. §5.9).
- Retry на embedding (`autoretry_for=(openai.RateLimitError,)`, `retry_backoff=True`).
- Метрики: Prometheus-экспортер для Celery (`celery-exporter`), Flower для дебага.
- Тесты задач без воркера: `task.apply(args=[...])` — синхронно выполняет в текущем процессе.
- PDF: новый шаг `extract_text(file_id) → content_id` перед `split_into_chunks`. Файлы — в S3/MinIO (не в Postgres).
- Поиск: отдельный endpoint `POST /search {query}` → embed query → `qdrant.search` → вернуть `chunk_id`-ы → подтянуть тексты из Postgres.
