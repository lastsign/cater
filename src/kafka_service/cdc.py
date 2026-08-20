"""CDC sink Postgres -> Qdrant: removes orphaned vectors.

The problem: `Chunk.id` is exactly the point id in Qdrant (see `indexer._build_points`),
but deleting a chunk row (including via a cascade from `Content`) knows nothing about
Qdrant. The points stay in the collection forever and surface in search results.

The solution: Debezium reads the WAL of the `public.chunks` table and publishes events
to `cater.public.chunks`; this consumer takes the PK out of delete events and removes
the corresponding points. Application code does not change at all - there is still a
single source of truth (Postgres) and Qdrant catches up with it through the log.

CDC only captures changes made after the slot was created (`snapshot.mode=no_data`).
Orphans accumulated earlier are cleaned up by a one-off `sweep_orphans`.
"""

from __future__ import annotations

import json
import logging
import threading
import time

from confluent_kafka import KafkaError, Message

from src.kafka_service.config import (
    CDC_COLLECTIONS,
    CDC_COLLECTIONS_TTL_S,
    CDC_DELETE_BATCH,
    CDC_LINGER_S,
    DEFAULT_COLLECTION,
    GROUP_CDC_QDRANT,
    POLL_TIMEOUT_S,
    TOPIC_CDC_CHUNKS,
)
from src.kafka_service.sync_client import build_consumer

log = logging.getLogger(__name__)

_collections_cache: tuple[float, tuple[str, ...]] = (0.0, ())


def _target_collections() -> tuple[str, ...]:
    if CDC_COLLECTIONS:
        return CDC_COLLECTIONS

    global _collections_cache
    ts, cached = _collections_cache
    now = time.monotonic()
    if cached and now - ts < CDC_COLLECTIONS_TTL_S:
        return cached

    from src.embedder.qdrant import client

    names = tuple(c.name for c in client().get_collections().collections)
    _collections_cache = (now, names)
    return names


def _delete_points(point_ids: list[str]) -> None:
    """Deleting by id is idempotent: a missing point is a no-op, not an error.

    That is precisely what allows committing the offset after the delete: a redelivery
    of the same event simply finds nothing.
    """
    from qdrant_client.http import models

    from src.embedder.qdrant import client

    qc = client()
    selector = models.PointIdsList(points=point_ids)
    for collection in _target_collections():
        qc.delete(collection_name=collection, points_selector=selector, wait=True)


def _delete_with_retries(point_ids: list[str], attempts: int = 3) -> None:
    """Survives a Qdrant blip without hanging: ~3s of pausing in total.

    Retrying longer is not allowed - the consumer is not polling the broker and a long
    pause would drop it from the group via max.poll.interval.ms. If Qdrant is really
    down it is better to crash: the offset is uncommitted, the supervisor restarts the
    process and the events are replayed.
    """
    for attempt in range(1, attempts + 1):
        try:
            _delete_points(point_ids)
            return
        except Exception as exc:
            if attempt == attempts:
                raise
            log.warning("cdc: delete failed (%d/%d): %s", attempt, attempts, exc)
            time.sleep(0.5 * 2 ** (attempt - 1))


def _deleted_chunk_id(raw: bytes) -> str | None:
    """The id of the deleted row from a Debezium event (op=d), otherwise None.

    The format is the unwrapped envelope (schemas.enable=false):
    {"before": {...}, "after": null, "op": "d", "source": {...}}.
    With REPLICA IDENTITY DEFAULT, `before` holds only the PK - which is enough for us.
    """
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        log.error("cdc: could not parse event: %s", raw[:200])
        return None

    op = event.get("op")
    if op == "d":
        return (event.get("before") or {}).get("id")
    if op == "t":
        # TRUNCATE carries no row ids - there is nothing to synchronize from it.
        log.warning(
            "cdc: TRUNCATE on chunks, points stayed in Qdrant - a sweep is needed"
        )
    return None


def run_cdc_sync(stop: threading.Event | None = None) -> None:
    stop = stop or threading.Event()
    consumer = build_consumer(
        GROUP_CDC_QDRANT, [TOPIC_CDC_CHUNKS], **{"auto.offset.reset": "earliest"}
    )
    pending: list[str] = []
    last: Message | None = None
    deadline = time.monotonic() + CDC_LINGER_S

    def flush() -> None:
        """Delete in Qdrant first, move the offset second - otherwise the delete is lost."""
        nonlocal pending, last, deadline
        if pending:
            _delete_with_retries(pending)
            log.info("cdc: points deleted: %d", len(pending))
            pending = []
        if last is not None:
            consumer.commit(message=last, asynchronous=False)
            last = None
        deadline = time.monotonic() + CDC_LINGER_S

    log.info("cdc: listening on %s as group %s", TOPIC_CDC_CHUNKS, GROUP_CDC_QDRANT)
    try:
        while not stop.is_set():
            msg = consumer.poll(POLL_TIMEOUT_S)
            if msg is None:
                if time.monotonic() >= deadline:
                    flush()
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("cdc: consumer error: %s", msg.error())
                continue

            last = msg
            # value=None is the tombstone Debezium sends right after a delete (for
            # compaction). There is no work in it, but the offset must still advance.
            if msg.value():
                chunk_id = _deleted_chunk_id(msg.value())
                if chunk_id:
                    pending.append(chunk_id)

            if len(pending) >= CDC_DELETE_BATCH or time.monotonic() >= deadline:
                flush()
    finally:
        try:
            flush()
        except Exception:
            log.exception("cdc: final flush failed, events will be replayed")
        consumer.close()
        log.info("cdc: stopped")


def sweep_orphans(
    collection: str = DEFAULT_COLLECTION,
    batch: int = 1000,
    dry_run: bool = False,
) -> int:
    """One-off reconciliation of a collection with the DB: deletes points no longer in `chunks`.

    Needed for two cases: clearing out orphans accumulated before CDC was enabled, and
    covering gaps (TRUNCATE, a lost slot, restoring the DB from a dump).
    """
    from qdrant_client.http import models

    from src.embedder.qdrant import client
    from src.storage import existing_chunk_ids

    qc = client()
    offset = None
    orphans_total = 0
    scanned = 0

    while True:
        points, offset = qc.scroll(
            collection_name=collection,
            limit=batch,
            offset=offset,
            with_payload=False,
            with_vectors=False,
        )
        if not points:
            break

        ids = [str(p.id) for p in points]
        scanned += len(ids)
        alive = existing_chunk_ids(ids)
        orphans = [i for i in ids if i not in alive]

        if orphans:
            orphans_total += len(orphans)
            log.info("sweep: orphans in batch %d/%d", len(orphans), len(ids))
            if not dry_run:
                qc.delete(
                    collection_name=collection,
                    points_selector=models.PointIdsList(points=orphans),
                    wait=True,
                )

        if offset is None:
            break

    log.info(
        "sweep: scanned %d, orphans %d%s",
        scanned,
        orphans_total,
        " (dry-run)" if dry_run else " - deleted",
    )
    return orphans_total
