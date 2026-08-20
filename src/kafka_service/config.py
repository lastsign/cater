import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "cater")

# --- топики -----------------------------------------------------------------
# Пайплайн: index.requests -> content.fetched -> chunks.ready -> index.done.
# index.events — побочная шина статусов (для WS/метрик), index.dlq — «мёртвые» сообщения.
TOPIC_INDEX_REQUESTS = os.getenv("KAFKA_TOPIC_REQUESTS", "index.requests")
TOPIC_CONTENT_FETCHED = os.getenv("KAFKA_TOPIC_FETCHED", "content.fetched")
TOPIC_CHUNKS_READY = os.getenv("KAFKA_TOPIC_CHUNKS", "chunks.ready")
TOPIC_INDEX_DONE = os.getenv("KAFKA_TOPIC_DONE", "index.done")
TOPIC_INDEX_EVENTS = os.getenv("KAFKA_TOPIC_EVENTS", "index.events")
TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "index.dlq")

ALL_TOPICS = (
    TOPIC_INDEX_REQUESTS,
    TOPIC_CONTENT_FETCHED,
    TOPIC_CHUNKS_READY,
    TOPIC_INDEX_DONE,
    TOPIC_INDEX_EVENTS,
    TOPIC_DLQ,
)

# --- consumer-группы (по одной на стадию) -----------------------------------
GROUP_FETCH = os.getenv("KAFKA_GROUP_FETCH", "cater.fetch")
GROUP_CHUNK = os.getenv("KAFKA_GROUP_CHUNK", "cater.chunk")
GROUP_EMBED = os.getenv("KAFKA_GROUP_EMBED", "cater.embed")

# index.events читают двое, и группы у них принципиально разные:
#  - проектор пишет статусы в Postgres — общая группа, каждое событие обрабатывает
#    ровно один инстанс (иначе N реплик API дублируют запись);
#  - WS-pump раздаёт события подписчикам — уникальная группа на процесс, каждому
#    инстансу нужны ВСЕ события, а не своя доля партиций (см. async_client.iter_events).
GROUP_EVENTS_PROJECTOR = os.getenv(
    "KAFKA_GROUP_EVENTS_PROJECTOR", "cater.events.projector"
)

# --- топология --------------------------------------------------------------
# Партиций должно быть >= числа воркеров стадии, иначе лишние простаивают.
TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "6"))
TOPIC_REPLICATION = int(os.getenv("KAFKA_TOPIC_REPLICATION", "1"))
TOPIC_RETENTION_MS = int(
    os.getenv("KAFKA_TOPIC_RETENTION_MS", str(7 * 24 * 3600 * 1000))
)

# --- тайминги воркера -------------------------------------------------------
POLL_TIMEOUT_S = float(os.getenv("KAFKA_POLL_TIMEOUT_S", "1.0"))
FLUSH_TIMEOUT_S = float(os.getenv("KAFKA_FLUSH_TIMEOUT_S", "15.0"))
HTTP_TIMEOUT_S = float(os.getenv("KAFKA_HTTP_TIMEOUT_S", "30.0"))

# max.poll.interval.ms: если handler работает дольше — брокер выкинет консьюмера
# из группы и отдаст партицию соседу (получим двойную обработку).
# embed идёт минутами, поэтому у него отдельный лимит.
MAX_POLL_INTERVAL_MS = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", str(5 * 60 * 1000)))
EMBED_MAX_POLL_INTERVAL_MS = int(
    os.getenv("KAFKA_EMBED_MAX_POLL_INTERVAL_MS", str(30 * 60 * 1000))
)

DEFAULT_COLLECTION = os.getenv("QDRANT_COLLECTION", "hugging-face-collection")

# --- CDC: Debezium (WAL Postgres) -> удаление точек в Qdrant ----------------
# Имя топика собирает сам Debezium: <topic.prefix>.<schema>.<table>.
CDC_TOPIC_PREFIX = os.getenv("DEBEZIUM_TOPIC_PREFIX", "cater")
TOPIC_CDC_CHUNKS = os.getenv(
    "KAFKA_TOPIC_CDC_CHUNKS", f"{CDC_TOPIC_PREFIX}.public.chunks"
)
GROUP_CDC_QDRANT = os.getenv("KAFKA_GROUP_CDC", "cater.qdrant-sync")

# Удаление документа каскадит в тысячи строк chunks — значит и в тысячи событий.
# Копим их и бьём одним запросом в Qdrant.
CDC_DELETE_BATCH = int(os.getenv("CDC_DELETE_BATCH", "512"))
CDC_LINGER_S = float(os.getenv("CDC_LINGER_S", "2.0"))

# В какие коллекции бить. Пусто = все коллекции Qdrant: событие удаления чанка
# не знает, куда его индексировали (документ мог уехать в несколько коллекций).
CDC_COLLECTIONS = tuple(
    c.strip() for c in os.getenv("CDC_COLLECTIONS", "").split(",") if c.strip()
)
CDC_COLLECTIONS_TTL_S = float(os.getenv("CDC_COLLECTIONS_TTL_S", "60"))


def producer_config(**overrides) -> dict:
    """acks=all + идемпотентный продюсер: без дублей при внутренних ретраях librdkafka."""
    cfg = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": CLIENT_ID,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 20,
        "retries": 10,
        "delivery.timeout.ms": 120_000,
    }
    cfg.update(overrides)
    return cfg


def consumer_config(group_id: str, **overrides) -> dict:
    """Ручной коммит: оффсет двигаем только после успешного handler'а (at-least-once)."""
    cfg = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": CLIENT_ID,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.partition.eof": False,
        "max.poll.interval.ms": MAX_POLL_INTERVAL_MS,
        "session.timeout.ms": 45_000,
        # По одному сообщению за раз: стадии тяжёлые, батчить нечего.
        "fetch.max.bytes": 10 * 1024 * 1024,
    }
    cfg.update(overrides)
    return cfg
