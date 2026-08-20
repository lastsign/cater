"""Создание топиков. Автосоздание в брокере выключено — топология задаётся явно."""

from __future__ import annotations

import logging

from confluent_kafka.admin import AdminClient, NewTopic

from src.kafka_service.config import (
    ALL_TOPICS,
    BOOTSTRAP_SERVERS,
    TOPIC_DLQ,
    TOPIC_PARTITIONS,
    TOPIC_REPLICATION,
    TOPIC_RETENTION_MS,
)

log = logging.getLogger(__name__)


def ensure_topics(topics: tuple[str, ...] = ALL_TOPICS) -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    existing = set(admin.list_topics(timeout=10).topics)
    missing = [t for t in topics if t not in existing]
    if not missing:
        log.info("all topics exist: %s", ", ".join(topics))
        return

    new_topics = [
        NewTopic(
            name,
            num_partitions=TOPIC_PARTITIONS,
            replication_factor=TOPIC_REPLICATION,
            config={
                # DLQ храним дольше: разбор падений обычно не в тот же день.
                "retention.ms": str(
                    TOPIC_RETENTION_MS * (4 if name == TOPIC_DLQ else 1)
                ),
            },
        )
        for name in missing
    ]
    for name, fut in admin.create_topics(new_topics).items():
        fut.result()
        log.info("created topic %s", name)
