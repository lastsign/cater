"""Синхронные обёртки над confluent-kafka (librdkafka) — для стадийных воркеров."""

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
    """Тонкая обёртка: сериализация Envelope + poll для колбэков доставки.

    produce() у librdkafka асинхронный — сообщение уходит в локальную очередь.
    Гарантия «записано в брокер» появляется только после flush(), поэтому
    воркер всегда делает flush перед коммитом оффсета.
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
        # Неблокирующий poll — вызывает накопившиеся delivery-колбэки.
        self._producer.poll(0)

    def send_raw(self, topic: str, value: bytes, key: str | None = None) -> None:
        """Отправка уже сериализованного тела — для DLQ-реплея, без пересборки Envelope."""
        self._producer.produce(
            topic,
            value=value,
            key=key.encode("utf-8") if key else None,
            on_delivery=_on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = FLUSH_TIMEOUT_S) -> int:
        """Возвращает число НЕдоставленных сообщений. Не 0 — считаем отправку провалившейся."""
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
