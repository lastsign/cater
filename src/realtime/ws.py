"""WS-ручка статуса запроса: снапшот + живой поток из index.events.

Порядок операций в stream_request — единственное, что здесь по-настоящему важно:

    accept -> subscribe -> snapshot -> live

Подписка ДО чтения снапшота: между SELECT и подпиской успевает пролететь
событие, и оно бы пропало. Обратная сторона — дубли (событие есть и в снапшоте,
и в очереди), поэтому дедуп по event_id, а не по порядку.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections import deque
from collections.abc import Awaitable, Callable
from uuid import UUID

from fastapi import WebSocket, WebSocketDisconnect

from src.realtime.dispatcher import Subscription, dispatcher
from src.storage import load_request_snapshot

log = logging.getLogger(__name__)

# Пауза, после которой шлём ping: WS без трафика рубят прокси (nginx — 60с).
HEARTBEAT_S = float(os.getenv("WS_HEARTBEAT_S", "25"))
# Сколько event_id помнить для дедупа снапшот/replay/live.
SEEN_MAX = 256


async def _wait_disconnect(ws: WebSocket) -> None:
    """Единственная задача — заметить, что клиент ушёл.

    Читать входящие всё равно надо: без receive Starlette не увидит close-фрейм,
    и соединение останется висеть вместе с подпиской.
    """
    try:
        while True:
            await ws.receive_text()
    except (WebSocketDisconnect, RuntimeError):
        return


async def stream_request(
    ws: WebSocket,
    request_id: UUID,
    snapshot_loader: Callable[[UUID], Awaitable[dict]] = load_request_snapshot,
    heartbeat_s: float = HEARTBEAT_S,
    close_on_final: bool = True,
) -> None:
    await ws.accept()
    sub: Subscription = dispatcher.subscribe(request_id)
    seen: deque[str] = deque(maxlen=SEEN_MAX)
    disconnect = asyncio.create_task(_wait_disconnect(ws))
    pending: asyncio.Task[dict] | None = None

    try:
        snapshot = await snapshot_loader(request_id)
        for entry in snapshot.get("history", ()):
            if entry.get("event_id"):
                seen.append(entry["event_id"])
        await ws.send_json(snapshot)
        if close_on_final and snapshot.get("final"):
            return

        while True:
            # Задачу ожидания события держим между итерациями: пересоздавать её
            # после каждого heartbeat нельзя — cancel съел бы уже вынутое из
            # очереди событие.
            if pending is None:
                pending = asyncio.create_task(sub.get())
            done, _ = await asyncio.wait(
                {pending, disconnect},
                timeout=heartbeat_s,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if disconnect in done:
                break
            if pending not in done:
                await ws.send_json({"type": "ping"})
                continue

            event = pending.result()
            pending = None
            if event.get("event_id") in seen:
                continue
            seen.append(event.get("event_id"))
            await ws.send_json(event)
            if close_on_final and event.get("final"):
                break
    except WebSocketDisconnect:
        pass
    finally:
        if pending is not None:
            pending.cancel()
        disconnect.cancel()
        dispatcher.unsubscribe(request_id, sub)
        if sub.dropped:
            log.warning(
                "ws request_id=%s dropped %d events (slow client)",
                request_id,
                sub.dropped,
            )
        try:
            await ws.close()
        except RuntimeError:
            pass  # уже закрыт клиентом
