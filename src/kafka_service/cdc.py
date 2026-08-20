"""CDC-синк Postgres → Qdrant: убирает осиротевшие векторы.

Проблема: `Chunk.id` — это ровно id точки в Qdrant (см. `indexer._build_points`),
но удаление строки чанка (в том числе каскадом от `Content`) ничего не знает про
Qdrant. Точки остаются в коллекции навсегда и всплывают в поиске.

Решение: Debezium читает WAL таблицы `public.chunks` и публикует события в
`cater.public.chunks`; этот консьюмер берёт из delete-событий PK и удаляет
соответствующие точки. Прикладной код при этом не меняется вообще — источник
правды остаётся один (Postgres), а Qdrant догоняет его по журналу.

CDC ловит только изменения с момента создания слота (`snapshot.mode=no_data`).
Сироты, накопившиеся раньше, убираются разовым `sweep_orphans`.
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
    """Удаление по id идемпотентно: несуществующая точка — не ошибка, а no-op.

    Именно это позволяет коммитить оффсет после удаления: повторная доставка
    того же события просто ничего не найдёт.
    """
    from qdrant_client.http import models

    from src.embedder.qdrant import client

    qc = client()
    selector = models.PointIdsList(points=point_ids)
    for collection in _target_collections():
        qc.delete(collection_name=collection, points_selector=selector, wait=True)


def _delete_with_retries(point_ids: list[str], attempts: int = 3) -> None:
    """Переживает моргание Qdrant, но не зависает: суммарная пауза ~3с.

    Дольше ретраить нельзя — консьюмер не опрашивает брокер и на длинной паузе
    вылетит из группы по max.poll.interval.ms. Если Qdrant лежит всерьёз, лучше
    упасть: оффсет не закоммичен, супервизор перезапустит, события переиграются.
    """
    for attempt in range(1, attempts + 1):
        try:
            _delete_points(point_ids)
            return
        except Exception as exc:
            if attempt == attempts:
                raise
            log.warning("cdc: удаление не прошло (%d/%d): %s", attempt, attempts, exc)
            time.sleep(0.5 * 2 ** (attempt - 1))


def _deleted_chunk_id(raw: bytes) -> str | None:
    """id удалённой строки из Debezium-события (op=d), иначе None.

    Формат — «развёрнутый» конверт (schemas.enable=false):
    {"before": {...}, "after": null, "op": "d", "source": {...}}.
    При REPLICA IDENTITY DEFAULT в `before` лежит только PK — нам этого хватает.
    """
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        log.error("cdc: не разобрал событие: %s", raw[:200])
        return None

    op = event.get("op")
    if op == "d":
        return (event.get("before") or {}).get("id")
    if op == "t":
        # TRUNCATE не даёт id строк — синхронизировать по нему нечего.
        log.warning("cdc: TRUNCATE на chunks, точки в Qdrant остались — нужен sweep")
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
        """Сначала удаляем в Qdrant, потом двигаем оффсет — иначе удаление теряется."""
        nonlocal pending, last, deadline
        if pending:
            _delete_with_retries(pending)
            log.info("cdc: удалено точек: %d", len(pending))
            pending = []
        if last is not None:
            consumer.commit(message=last, asynchronous=False)
            last = None
        deadline = time.monotonic() + CDC_LINGER_S

    log.info("cdc: слушаю %s как группа %s", TOPIC_CDC_CHUNKS, GROUP_CDC_QDRANT)
    try:
        while not stop.is_set():
            msg = consumer.poll(POLL_TIMEOUT_S)
            if msg is None:
                if time.monotonic() >= deadline:
                    flush()
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("cdc: ошибка консьюмера: %s", msg.error())
                continue

            last = msg
            # value=None — tombstone, который Debezium шлёт следом за delete
            # (для compaction). Работы в нём нет, но оффсет двигать надо.
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
            log.exception("cdc: финальный flush не прошёл, события переиграются")
        consumer.close()
        log.info("cdc: остановлен")


def sweep_orphans(
    collection: str = DEFAULT_COLLECTION,
    batch: int = 1000,
    dry_run: bool = False,
) -> int:
    """Разовая сверка коллекции с БД: удаляет точки, которых уже нет в `chunks`.

    Нужна для двух случаев: разгрести сирот, накопившихся до включения CDC, и
    подстраховать пропуски (TRUNCATE, потерянный слот, переливка БД из дампа).
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
            log.info("sweep: сирот в пачке %d/%d", len(orphans), len(ids))
            if not dry_run:
                qc.delete(
                    collection_name=collection,
                    points_selector=models.PointIdsList(points=orphans),
                    wait=True,
                )

        if offset is None:
            break

    log.info(
        "sweep: просмотрено %d, сирот %d%s",
        scanned,
        orphans_total,
        " (dry-run)" if dry_run else " — удалены",
    )
    return orphans_total
