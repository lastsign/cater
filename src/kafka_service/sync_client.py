"""Synchronous wrappers around confluent-kafka (librdkafka) - for the stage workers."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from confluent_kafka import Consumer, KafkaException, Message, Producer

from src.kafka_service.config import (
    FLUSH_TIMEOUT_S,
    consumer_config,
    producer_config,
)
from src.kafka_service.schemas import Envelope

log = logging.getLogger(__name__)


def _on_delivery(err, msg: Message) -> None:
    if err is not None:
        log.error("kafka delivery failed: topic=%s error=%s", msg.topic(), err)


class SyncProducer:
    """Thin wrapper: Envelope serialization plus a poll for delivery callbacks.

    librdkafka's produce() is asynchronous - the message goes to a local queue. The
    "written to the broker" guarantee only appears after flush(), which is why the
    worker always flushes before committing an offset.
    """

    def __init__(self, **overrides):
        self._producer = Producer(producer_config(**overrides))

    def send(self, topic: str, envelope: Envelope, key: str | None = None) -> None:
        self._producer.produce(
            topic,
            value=envelope.to_bytes(),
            key=key.encode("utf-8") if key else None,
            headers=[
                ("event_type", envelope.type.encode("utf-8")),
                ("request_id", str(envelope.request_id).encode("utf-8")),
            ],
            on_delivery=_on_delivery,
        )
        # Non-blocking poll - fires the delivery callbacks that have piled up.
        self._producer.poll(0)

    def send_raw(self, topic: str, value: bytes, key: str | None = None) -> None:
        """Send an already serialized body - for DLQ replay, without rebuilding the Envelope."""
        self._producer.produce(
            topic,
            value=value,
            key=key.encode("utf-8") if key else None,
            on_delivery=_on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = FLUSH_TIMEOUT_S) -> int:
        """Returns the number of UNdelivered messages. Non-zero means the send failed."""
        return self._producer.flush(timeout)

    def flush_or_raise(self, timeout: float = FLUSH_TIMEOUT_S) -> None:
        remaining = self.flush(timeout)
        if remaining:
            raise KafkaException(
                f"{remaining} messages not delivered within {timeout}s"
            )

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self.flush()


def build_consumer(group_id: str, topics: Iterable[str], **overrides) -> Consumer:
    consumer = Consumer(consumer_config(group_id, **overrides))
    consumer.subscribe(list(topics))
    return consumer
