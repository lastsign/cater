"""Проекция сообщений index.events в плоский вид для WS и для БД.

Kafka-конверт (`Envelope[StatusEvent]`) неудобно отдавать в браузер и хранить в
Postgres: одна и та же форма нужна и pump'у (WS), и проектору (снапшот), поэтому
она живёт здесь, а не в двух местах.
"""

from __future__ import annotations

import logging

from src.kafka_service.schemas import Envelope, StatusEvent

log = logging.getLogger(__name__)

# Состояния, после которых по этому request_id больше ничего не придёт:
# indexed — документ в Qdrant, skipped — уже был проиндексирован, failed — DLQ.
TERMINAL_STATUSES = frozenset({"indexed", "skipped", "failed"})

# Порядок прогресса. Нужен проектору: события двух стадий могут прийти
# в обратном порядке (fetch-падение уходит в index.events с ключом url,
# остальные — с ключом doc_id, то есть в другую партицию).
STATUS_RANK = {"pending": 0, "fetched": 1, "chunked": 2, "indexed": 3}


def is_final(status: str | None) -> bool:
    return status in TERMINAL_STATUSES


def status_view(env: Envelope[StatusEvent], seq: int | None = None) -> dict:
    """Плоское событие статуса. seq — оффсет в index.events (для дедупа/отладки)."""
    p = env.payload
    return {
        "type": "status",
        "request_id": str(env.request_id),
        "event_id": str(env.event_id),
        "stage": p.stage,
        "status": p.status,
        "doc_id": p.doc_id,
        "url": p.url,
        "detail": p.detail,
        "at": env.occurred_at.isoformat(),
        "seq": seq,
        "final": is_final(p.status),
    }


def parse_status_event(raw: bytes | None, seq: int | None = None) -> dict | None:
    """Байты сообщения -> плоское событие. None, если тело не статусное/битое."""
    try:
        env = Envelope[StatusEvent].model_validate_json(raw or b"")
    except Exception:
        log.exception("bad status event at seq=%s", seq)
        return None
    return status_view(env, seq)


def supersedes(old_status: str | None, new_status: str | None) -> bool:
    """Двигать ли статус запроса вперёд.

    Терминальное состояние применяем всегда (это конец истории), прогресс —
    только вперёд по STATUS_RANK, но после failed разрешаем начать заново:
    replay из DLQ пролетает по тем же стадиям с тем же request_id.
    """
    if old_status is None or old_status == "failed":
        return True
    if is_final(new_status):
        return True
    return STATUS_RANK.get(new_status, -1) > STATUS_RANK.get(old_status, -1)
