"""Стадийный воркер: consume -> handler -> produce -> commit.

Порядок в конце цикла принципиален: сначала flush продюсера, потом commit оффсета.
Обратный порядок теряет работу — оффсет уже сдвинут, а сообщение следующей стадии
может не долететь. Текущий порядок даёт at-least-once: в худшем случае стадия
отработает дважды (все три стадии идемпотентны — см. stages.py).
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from collections.abc import Sequence

from confluent_kafka import Consumer, KafkaError, Message
from pydantic import ValidationError

from src.kafka_service.config import (
    FLUSH_TIMEOUT_S,
    POLL_TIMEOUT_S,
    TOPIC_DLQ,
    TOPIC_INDEX_EVENTS,
)
from src.kafka_service.schemas import Envelope, StageFailed, StatusEvent
from src.kafka_service.stages import STAGES, Emit, Stage
from src.kafka_service.sync_client import SyncProducer, build_consumer

log = logging.getLogger(__name__)

RETRY_BASE_DELAY_S = 2.0


def _key(msg: Message) -> str | None:
    raw = msg.key()
    return raw.decode("utf-8", "replace") if raw else None


def _emit_all(producer: SyncProducer, emits: Sequence[Emit], env: Envelope) -> None:
    for e in emits:
        producer.send(e.topic, _wrap(e.type, e.payload, env), key=e.key)


def _wrap(type_: str, payload, env: Envelope | None) -> Envelope:
    """Наследует request_id/attempt исходного сообщения; для битого тела — новый request_id."""
    extra = {"request_id": env.request_id, "attempt": env.attempt} if env else {}
    return Envelope(type=type_, payload=payload, **extra)


def _to_dlq(
    producer: SyncProducer,
    stage: Stage,
    msg: Message,
    env: Envelope | None,
    exc: BaseException,
) -> None:
    failed = StageFailed(
        stage=stage.name,
        topic=msg.topic(),
        partition=msg.partition(),
        offset=msg.offset(),
        error=str(exc),
        error_type=type(exc).__name__,
        key=_key(msg),
        raw=(msg.value() or b"").decode("utf-8", "replace"),
    )
    producer.send(TOPIC_DLQ, _wrap("stage.failed", failed, env), key=_key(msg))

    doc_id = getattr(env.payload, "doc_id", None) if env else None
    url = getattr(env.payload, "url", None) if env else None
    status = StatusEvent(
        stage=stage.name, status="failed", doc_id=doc_id, url=url, detail=str(exc)
    )
    producer.send(
        TOPIC_INDEX_EVENTS, _wrap("index.status", status, env), key=doc_id or _key(msg)
    )


def _handle_with_retries(stage: Stage, env: Envelope) -> Sequence[Emit]:
    """Ретраи внутри процесса: без переотправки в Kafka и без сдвига оффсета.

    Пока идут попытки, партиция не обрабатывается — поэтому суммарное время
    ретраев обязано укладываться в max.poll.interval.ms стадии.
    """
    last: Exception | None = None
    for attempt in range(1, stage.max_attempts + 1):
        try:
            return stage.handler(env.payload, env)
        except Exception as exc:  # noqa: BLE001 — решение о фатальности принимаем ниже
            last = exc
            log.warning(
                "stage=%s attempt=%d/%d failed: %s",
                stage.name,
                attempt,
                stage.max_attempts,
                exc,
            )
            if attempt < stage.max_attempts:
                time.sleep(RETRY_BASE_DELAY_S * 2 ** (attempt - 1))
    assert last is not None
    raise last


def _process(
    stage: Stage, msg: Message, producer: SyncProducer, consumer: Consumer
) -> None:
    try:
        env = Envelope[stage.payload_model].model_validate_json(msg.value() or b"")
    except ValidationError as exc:
        # Битое сообщение ретраить бессмысленно — сразу в DLQ.
        log.error(
            "stage=%s malformed message at %s[%d]@%d: %s",
            stage.name,
            msg.topic(),
            msg.partition(),
            msg.offset(),
            exc,
        )
        _to_dlq(producer, stage, msg, None, exc)
        producer.flush_or_raise()
        consumer.commit(message=msg, asynchronous=False)
        return

    try:
        emits = _handle_with_retries(stage, env)
        _emit_all(producer, emits, env)
    except Exception as exc:
        log.exception(
            "stage=%s giving up on %s[%d]@%d",
            stage.name,
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )
        _to_dlq(producer, stage, msg, env, exc)

    producer.flush_or_raise(FLUSH_TIMEOUT_S)
    consumer.commit(message=msg, asynchronous=False)


def run_stage(stage_name: str, stop: threading.Event | None = None) -> None:
    stage = STAGES[stage_name]
    stop = stop or threading.Event()

    consumer = build_consumer(
        stage.group_id, stage.topics, **(stage.consumer_overrides or {})
    )
    producer = SyncProducer()
    log.info(
        "stage=%s consuming %s as group=%s", stage.name, stage.topics, stage.group_id
    )

    try:
        while not stop.is_set():
            msg = consumer.poll(POLL_TIMEOUT_S)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("stage=%s consumer error: %s", stage.name, msg.error())
                continue
            _process(stage, msg, producer, consumer)
    finally:
        producer.flush()
        # close() коммитит то, что уже сохранено, и корректно покидает группу —
        # ребаланс проходит сразу, без ожидания session.timeout.
        consumer.close()
        log.info("stage=%s stopped", stage.name)


def stop_event_with_signals() -> threading.Event:
    """Event, который взводится по SIGINT/SIGTERM — цикл дорабатывает сообщение и выходит."""
    stop = threading.Event()

    def _shutdown(signum, frame):
        log.info("signal %s received, draining", signum)
        stop.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    return stop


def run_stages(stage_names: Sequence[str]) -> None:
    """Несколько стадий в одном процессе (по потоку на стадию) — для локального запуска.

    В проде каждая стадия — отдельный процесс/под: у них разный профиль нагрузки
    (fetch — сеть, embed — GPU) и масштабировать их нужно независимо.
    """
    stop = stop_event_with_signals()

    threads = [
        threading.Thread(
            target=run_stage, args=(name, stop), name=f"stage-{name}", daemon=False
        )
        for name in stage_names
    ]
    for t in threads:
        t.start()
    try:
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=0.5)
    except KeyboardInterrupt:
        stop.set()
        for t in threads:
            t.join()
