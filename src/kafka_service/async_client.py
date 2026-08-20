"""Async side of Kafka (aiokafka) - for FastAPI: accepting requests and serving statuses.

The stage workers run on confluent-kafka (sync); this module is only for the API
process: the producer publishes index.requests, the consumer reads index.events and
pushes them into the WS.
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
    """One producer per process. start/stop are hooked onto the FastAPI lifespan."""

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
        # send_and_wait waits for the broker's acknowledgement: we return an HTTP
        # response only once the request is really accepted, otherwise the client gets
        # a task_id pointing at nothing.
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
        """Publishes an indexing request and returns the request_id for tracking."""
        envelope = Envelope(
            request_id=request_id or uuid.uuid4(),
            type="index.requested",
            payload=IndexRequest(
                url=url, collection=collection or DEFAULT_COLLECTION, force=force
            ),
        )
        # Key = url: repeated requests for the same url go to one partition, in order.
        await self.send(TOPIC_INDEX_REQUESTS, envelope, key=url)
        return envelope.request_id


producer = AsyncProducer()


async def iter_events(
    group_id: str | None = None,
    topics: tuple[str, ...] = (TOPIC_INDEX_EVENTS,),
) -> AsyncIterator[dict]:
    """Reads status events in flat form (kafka_service.events.status_view).

    group_id=None means a unique group per process. That is exactly what WS fan-out
    needs: every API instance must receive all events, not its share of partitions
    (the projector is the one taking a share, its group is shared).

    latest: there is no history to catch up on here - the WS serves the past from the
    snapshot rather than from the topic, otherwise every API restart would replay the
    entire status feed.
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
    """Kafka -> WS bridge: pours index.events into the given publish (realtime.dispatcher).

    Lives in the FastAPI lifespan; reconnects when the connection drops. publish must
    not block on a client - otherwise one slow WS builds up lag for the whole group
    (see realtime.dispatcher.Subscription.offer).
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
