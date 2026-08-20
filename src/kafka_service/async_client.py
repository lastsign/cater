"""Async-сторона Kafka (aiokafka) — для FastAPI: приём запросов и раздача статусов.

Воркеры стадий на confluent-kafka (sync), здесь только API-процесс:
producer публикует index.requests, consumer читает index.events и пушит в WS.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from src.kafka_service.config import (
    BOOTSTRAP_SERVERS,
    CLIENT_ID,
    DEFAULT_COLLECTION,
    TOPIC_INDEX_EVENTS,
    TOPIC_INDEX_REQUESTS,
)
from src.kafka_service.events import parse_status_event
from src.kafka_service.schemas import Envelope, IndexRequest

log = logging.getLogger(__name__)


class AsyncProducer:
    """Один продюсер на процесс. start/stop вешаются на lifespan FastAPI."""

    def __init__(self, **overrides):
        self._kwargs = {
            "bootstrap_servers": BOOTSTRAP_SERVERS,
            "client_id": f"{CLIENT_ID}-api",
            "acks": "all",
            "enable_idempotence": True,
            "compression_type": "lz4",
            "linger_ms": 20,
            **overrides,
        }
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        if self._producer is None:
            self._producer = AIOKafkaProducer(**self._kwargs)
            await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def send(
        self, topic: str, envelope: Envelope, key: str | None = None
    ) -> None:
        if self._producer is None:
            raise RuntimeError("AsyncProducer is not started")
        # send_and_wait ждёт подтверждения от брокера: HTTP-ответ отдаём только
        # когда запрос реально принят, иначе клиент получит task_id в никуда.
        await self._producer.send_and_wait(
            topic,
            value=envelope.to_bytes(),
            key=key.encode("utf-8") if key else None,
            headers=[
                ("event_type", envelope.type.encode("utf-8")),
                ("request_id", str(envelope.request_id).encode("utf-8")),
            ],
        )

    async def submit_index_request(
        self,
        url: str,
        collection: str | None = None,
        force: bool = False,
        request_id: uuid.UUID | None = None,
    ) -> uuid.UUID:
        """Публикует запрос на индексацию, возвращает request_id для отслеживания."""
        envelope = Envelope(
            request_id=request_id or uuid.uuid4(),
            type="index.requested",
            payload=IndexRequest(
                url=url, collection=collection or DEFAULT_COLLECTION, force=force
            ),
        )
        # Ключ = url: повторные запросы одного url идут в одну партицию по порядку.
        await self.send(TOPIC_INDEX_REQUESTS, envelope, key=url)
        return envelope.request_id


producer = AsyncProducer()


async def iter_events(
    group_id: str | None = None,
    topics: tuple[str, ...] = (TOPIC_INDEX_EVENTS,),
) -> AsyncIterator[dict]:
    """Читает статусные события в плоском виде (kafka_service.events.status_view).

    group_id=None — уникальная группа на процесс. Для WS-раздачи нужна именно
    она: каждый API-инстанс должен получить все события, а не свою долю партиций
    (свою долю берёт проектор, у него группа общая).

    latest: догонять историю тут нечего — WS отдаёт прошлое из снапшота, а не
    из топика, иначе каждый рестарт API прокручивал бы всю ленту статусов.
    """
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id=f"{CLIENT_ID}-api",
        group_id=group_id or f"cater.events.{uuid.uuid4()}",
        auto_offset_reset="latest",
        enable_auto_commit=group_id is not None,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            view = parse_status_event(msg.value, msg.offset)
            if view is not None:
                yield view
    finally:
        await consumer.stop()


async def run_event_pump(
    stop: asyncio.Event,
    publish: Callable[[uuid.UUID, dict], Awaitable[None]],
) -> None:
    """Мост Kafka -> WS: льёт index.events в переданный publish (realtime.dispatcher).

    Живёт в lifespan FastAPI; при обрыве переподключается. publish обязан не
    блокироваться на клиенте — иначе один медленный WS собирает лаг на всей
    группе (см. realtime.dispatcher.Subscription.offer).
    """
    while not stop.is_set():
        try:
            async for event in iter_events():
                if stop.is_set():
                    break
                await publish(uuid.UUID(event["request_id"]), event)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("kafka event pump crashed, reconnecting in 2s")
            await asyncio.sleep(2)
