"""Проектор index.events -> таблица request.

Зачем отдельный консьюмер, а не запись из стадий: стадии остаются чистыми
функциями над своим payload'ом и ничего не знают ни про request_id-снапшоты, ни
про WS. Событие уже есть в Kafka — проекция это просто ещё один его читатель.

Группа здесь ОБЩАЯ (`cater.events.projector`) — в отличие от WS-pump'а, который
берёт уникальную группу на процесс. Проектору нужно, чтобы каждое событие
обработал ровно один инстанс, а pump'у — чтобы каждый инстанс получил все.
"""

from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaConsumer

from src.kafka_service.config import (
    BOOTSTRAP_SERVERS,
    CLIENT_ID,
    GROUP_EVENTS_PROJECTOR,
    TOPIC_INDEX_EVENTS,
)
from src.kafka_service.events import parse_status_event

log = logging.getLogger(__name__)


async def run_status_projector(
    stop: asyncio.Event, group_id: str = GROUP_EVENTS_PROJECTOR
) -> None:
    """Пишет статусы в Postgres. Живёт в lifespan FastAPI или отдельным процессом.

    Порядок как у воркеров стадий: сначала запись в БД, потом коммит оффсета.
    При падении между ними событие приедет снова — record_status_event
    идемпотентна по event_id.
    """
    from src.storage import record_status_event

    while not stop.is_set():
        consumer = AIOKafkaConsumer(
            TOPIC_INDEX_EVENTS,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id=f"{CLIENT_ID}-projector",
            group_id=group_id,
            # earliest: пропущенные за простой статусы нужны — иначе в снапшоте
            # навсегда останется дырка (например, тот самый doc_id из fetch).
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await consumer.start()
        log.info("status projector started as group=%s", group_id)
        try:
            async for msg in consumer:
                if stop.is_set():
                    break
                view = parse_status_event(msg.value, msg.offset)
                if view is None:
                    await consumer.commit()  # битое тело ретраить бессмысленно
                    continue
                await record_status_event(view)
                await consumer.commit()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("status projector crashed, reconnecting in 2s")
            await asyncio.sleep(2)
        finally:
            await consumer.stop()
