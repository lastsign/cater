import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "cater")

# --- topics -----------------------------------------------------------------
# Pipeline: index.requests -> content.fetched -> chunks.ready -> index.done.
# index.events is a side channel of statuses (for WS/metrics), index.dlq holds dead messages.
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

# --- consumer groups (one per stage) ----------------------------------------
GROUP_FETCH = os.getenv("KAFKA_GROUP_FETCH", "cater.fetch")
GROUP_CHUNK = os.getenv("KAFKA_GROUP_CHUNK", "cater.chunk")
GROUP_EMBED = os.getenv("KAFKA_GROUP_EMBED", "cater.embed")

# index.events has two readers, and their groups differ fundamentally:
#  - the projector writes statuses to Postgres - a shared group, so exactly one
#    instance handles each event (otherwise N API replicas duplicate the write);
#  - the WS pump fans events out to subscribers - a unique group per process, since
#    every instance needs ALL events, not its share of partitions
#    (see async_client.iter_events).
GROUP_EVENTS_PROJECTOR = os.getenv(
    "KAFKA_GROUP_EVENTS_PROJECTOR", "cater.events.projector"
)

# --- topology ---------------------------------------------------------------
# Partition count must be >= the number of workers of a stage, otherwise the extra
# ones sit idle.
TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "6"))
TOPIC_REPLICATION = int(os.getenv("KAFKA_TOPIC_REPLICATION", "1"))
TOPIC_RETENTION_MS = int(
    os.getenv("KAFKA_TOPIC_RETENTION_MS", str(7 * 24 * 3600 * 1000))
)

# --- worker timings ---------------------------------------------------------
POLL_TIMEOUT_S = float(os.getenv("KAFKA_POLL_TIMEOUT_S", "1.0"))
FLUSH_TIMEOUT_S = float(os.getenv("KAFKA_FLUSH_TIMEOUT_S", "15.0"))
HTTP_TIMEOUT_S = float(os.getenv("KAFKA_HTTP_TIMEOUT_S", "30.0"))

# max.poll.interval.ms: if the handler runs longer, the broker kicks the consumer
# out of the group and hands the partition to a peer (we get double processing).
# embed takes minutes, so it has its own limit.
MAX_POLL_INTERVAL_MS = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", str(5 * 60 * 1000)))
EMBED_MAX_POLL_INTERVAL_MS = int(
    os.getenv("KAFKA_EMBED_MAX_POLL_INTERVAL_MS", str(30 * 60 * 1000))
)

DEFAULT_COLLECTION = os.getenv("QDRANT_COLLECTION", "hugging-face-collection")

# --- CDC: Debezium (Postgres WAL) -> deleting points in Qdrant --------------
# Debezium builds the topic name itself: <topic.prefix>.<schema>.<table>.
CDC_TOPIC_PREFIX = os.getenv("DEBEZIUM_TOPIC_PREFIX", "cater")
TOPIC_CDC_CHUNKS = os.getenv(
    "KAFKA_TOPIC_CDC_CHUNKS", f"{CDC_TOPIC_PREFIX}.public.chunks"
)
GROUP_CDC_QDRANT = os.getenv("KAFKA_GROUP_CDC", "cater.qdrant-sync")

# Deleting a document cascades into thousands of chunks rows - hence thousands of
# events. We accumulate them and hit Qdrant with a single request.
CDC_DELETE_BATCH = int(os.getenv("CDC_DELETE_BATCH", "512"))
CDC_LINGER_S = float(os.getenv("CDC_LINGER_S", "2.0"))

# Which collections to hit. Empty = all Qdrant collections: a chunk delete event
# does not know where it was indexed (a document may have gone into several).
CDC_COLLECTIONS = tuple(
    c.strip() for c in os.getenv("CDC_COLLECTIONS", "").split(",") if c.strip()
)
CDC_COLLECTIONS_TTL_S = float(os.getenv("CDC_COLLECTIONS_TTL_S", "60"))


def producer_config(**overrides) -> dict:
    """acks=all + idempotent producer: no duplicates on librdkafka's internal retries."""
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
    """Manual commit: the offset advances only after a successful handler (at-least-once)."""
    cfg = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": CLIENT_ID,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.partition.eof": False,
        "max.poll.interval.ms": MAX_POLL_INTERVAL_MS,
        "session.timeout.ms": 45_000,
        # One message at a time: the stages are heavy, there is nothing to batch.
        "fetch.max.bytes": 10 * 1024 * 1024,
    }
    cfg.update(overrides)
    return cfg
