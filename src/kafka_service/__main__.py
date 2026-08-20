"""CLI сервиса.

python -m src.kafka_service topics                 # создать топики
python -m src.kafka_service run fetch              # воркер одной стадии
python -m src.kafka_service run fetch chunk embed  # все стадии в одном процессе (dev)
python -m src.kafka_service submit <url>           # положить запрос в index.requests
python -m src.kafka_service tail index.events      # смотреть поток событий
python -m src.kafka_service replay                 # вернуть DLQ в исходные топики
python -m src.kafka_service run-projector          # index.events -> таблица request
python -m src.kafka_service run-cdc                # CDC-синк: удаления chunks -> Qdrant
python -m src.kafka_service sweep                  # разовая чистка осиротевших точек
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
import uuid

from src.kafka_service.admin import ensure_topics
from src.kafka_service.cdc import run_cdc_sync, sweep_orphans
from src.kafka_service.config import (
    DEFAULT_COLLECTION,
    POLL_TIMEOUT_S,
    TOPIC_DLQ,
    TOPIC_INDEX_EVENTS,
    TOPIC_INDEX_REQUESTS,
)
from src.kafka_service.schemas import Envelope, IndexRequest, StageFailed
from src.kafka_service.stages import STAGES
from src.kafka_service.sync_client import SyncProducer, build_consumer
from src.kafka_service.worker import run_stages, stop_event_with_signals

log = logging.getLogger("kafka_service")


def _submit(url: str, collection: str | None, force: bool) -> uuid.UUID:
    envelope = Envelope(
        type="index.requested",
        payload=IndexRequest(
            url=url, collection=collection or DEFAULT_COLLECTION, force=force
        ),
    )
    with SyncProducer() as producer:
        producer.send(TOPIC_INDEX_REQUESTS, envelope, key=url)
        producer.flush_or_raise()
    return envelope.request_id


def _tail(topic: str, group: str | None) -> None:
    consumer = build_consumer(
        group or f"cater.tail.{uuid.uuid4()}",
        [topic],
        **{"auto.offset.reset": "latest"},
    )
    print(f"tailing {topic} (Ctrl+C to stop)", file=sys.stderr)
    try:
        while True:
            msg = consumer.poll(POLL_TIMEOUT_S)
            if msg is None or msg.error():
                continue
            body = json.loads(msg.value())
            print(json.dumps(body, ensure_ascii=False))
            sys.stdout.flush()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def _replay(limit: int) -> int:
    """Читает DLQ и возвращает сообщения в топик, на котором они упали."""
    consumer = build_consumer("cater.dlq.replay", [TOPIC_DLQ])
    producer = SyncProducer()
    replayed = 0
    try:
        while replayed < limit:
            msg = consumer.poll(5.0)
            if msg is None:
                break
            if msg.error():
                continue
            env = Envelope[StageFailed].model_validate_json(msg.value())
            failed = env.payload
            if not failed.raw:
                consumer.commit(message=msg, asynchronous=False)
                continue

            original = json.loads(failed.raw)
            original["attempt"] = original.get("attempt", 0) + 1
            producer.send_raw(
                failed.topic,
                json.dumps(original, ensure_ascii=False).encode("utf-8"),
                key=failed.key,
            )
            producer.flush_or_raise()
            consumer.commit(message=msg, asynchronous=False)
            replayed += 1
            log.info("replayed to %s (stage=%s)", failed.topic, failed.stage)
    finally:
        producer.flush()
        consumer.close()
    return replayed


async def _run_projector() -> None:
    """Проектор отдельным процессом — альтернатива запуску его в lifespan FastAPI."""
    from src.kafka_service.projector import run_status_projector

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    task = asyncio.create_task(run_status_projector(stop))
    await stop.wait()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m src.kafka_service")
    parser.add_argument("--log-level", default="INFO")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("topics", help="создать недостающие топики")

    run_p = sub.add_parser("run", help="запустить воркеры стадий")
    run_p.add_argument("stages", nargs="+", choices=sorted(STAGES))
    run_p.add_argument("--ensure-topics", action="store_true")

    submit_p = sub.add_parser("submit", help="отправить url в index.requests")
    submit_p.add_argument("url")
    submit_p.add_argument("--collection", default=None)
    submit_p.add_argument("--force", action="store_true")

    tail_p = sub.add_parser("tail", help="печатать сообщения топика")
    tail_p.add_argument("topic", nargs="?", default=TOPIC_INDEX_EVENTS)
    tail_p.add_argument("--group", default=None)

    replay_p = sub.add_parser("replay", help="переотправить сообщения из DLQ")
    replay_p.add_argument("--limit", type=int, default=100)

    sub.add_parser(
        "run-projector", help="index.events -> таблица request (снапшоты для WS)"
    )

    sub.add_parser("run-cdc", help="CDC-синк: удаления chunks -> Qdrant")

    sweep_p = sub.add_parser("sweep", help="удалить осиротевшие точки в коллекции")
    sweep_p.add_argument("--collection", default=DEFAULT_COLLECTION)
    sweep_p.add_argument("--batch", type=int, default=1000)
    sweep_p.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.command == "topics":
        ensure_topics()
    elif args.command == "run":
        if args.ensure_topics:
            ensure_topics()
        run_stages(args.stages)
    elif args.command == "submit":
        print(_submit(args.url, args.collection, args.force))
    elif args.command == "tail":
        _tail(args.topic, args.group)
    elif args.command == "replay":
        print(f"replayed: {_replay(args.limit)}")
    elif args.command == "run-projector":
        asyncio.run(_run_projector())
    elif args.command == "run-cdc":
        run_cdc_sync(stop_event_with_signals())
    elif args.command == "sweep":
        removed = sweep_orphans(args.collection, args.batch, args.dry_run)
        print(f"orphans: {removed}{' (dry-run)' if args.dry_run else ''}")


if __name__ == "__main__":
    main()
