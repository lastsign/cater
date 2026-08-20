"""Projector of index.events -> the request table.

Why a separate consumer instead of writing from the stages: the stages stay pure
functions over their payload and know nothing about request_id snapshots or the WS.
The event is already in Kafka - the projection is simply one more reader of it.

The group here is SHARED (`cater.events.projector`) - unlike the WS pump, which takes
a unique group per process. The projector needs exactly one instance to handle each
event, while the pump needs every instance to receive all of them.
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
    """Writes statuses to Postgres. Lives in the FastAPI lifespan or as its own process.

    Same order as in the stage workers: write to the DB first, commit the offset
    second. On a crash in between, the event arrives again - record_status_event is
    idempotent by event_id.
    """
    from src.storage import record_status_event

    while not stop.is_set():
        consumer = AIOKafkaConsumer(
            TOPIC_INDEX_EVENTS,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id=f"{CLIENT_ID}-projector",
            group_id=group_id,
            # earliest: statuses missed during downtime are needed - otherwise the
            # snapshot keeps a permanent hole (e.g. that very doc_id from fetch).
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
                    await consumer.commit()  # retrying a broken body is pointless
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
