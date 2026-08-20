"""Фан-аут статусных событий по WS-подписчикам одного request_id.

Два решения, из которых состоит модуль:

1. Подписка — это очередь на соединение, а не сам WebSocket. publish никогда не
   ждёт клиента: медленный или залипший браузер терял бы события, но не тормозил
   бы Kafka-pump, а тормозил он бы его на всю группу index.events.
2. Буфер последних событий по request_id. Между «HTTP отдал request_id» и
   «клиент открыл WS» проходят десятки миллисекунд, а fetch укладывается в
   сотни — событие с doc_id уходило бы в никуда. При subscribe буфер отдаётся
   как replay, поэтому doc_id клиент получает даже если опоздал.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import OrderedDict, defaultdict, deque
from uuid import UUID

log = logging.getLogger(__name__)

# Сколько держать события запроса после последнего апдейта и сколько их помнить.
BUFFER_TTL_S = float(os.getenv("WS_BUFFER_TTL_S", "900"))
BUFFER_MAX_EVENTS = int(os.getenv("WS_BUFFER_MAX_EVENTS", "32"))
BUFFER_MAX_REQUESTS = int(os.getenv("WS_BUFFER_MAX_REQUESTS", "10000"))
# Глубина очереди соединения. Переполнение = клиент не читает; события с головы
# не выкидываем — теряем новые и считаем потери, чтобы это было видно в логах.
QUEUE_MAXSIZE = int(os.getenv("WS_QUEUE_MAXSIZE", "256"))


class Subscription:
    """Очередь событий одного WS-соединения."""

    __slots__ = ("_queue", "dropped", "request_id")

    def __init__(self, request_id: UUID, replay: tuple[dict, ...] = ()):
        self.request_id = request_id
        self.dropped = 0
        self._queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
        for event in replay:
            self.offer(event)

    def offer(self, event: dict) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped += 1

    async def get(self) -> dict:
        return await self._queue.get()


class WSDispatcher:
    def __init__(self) -> None:
        self._subs: dict[UUID, set[Subscription]] = defaultdict(set)
        # request_id -> (время последнего события, последние события)
        self._recent: OrderedDict[UUID, tuple[float, deque[dict]]] = OrderedDict()

    # --- подписки -----------------------------------------------------------

    def subscribe(self, request_id: UUID) -> Subscription:
        """Подписка + replay буфера. Синхронная: в одном loop'е лок не нужен."""
        sub = Subscription(request_id, self.replay(request_id))
        self._subs[request_id].add(sub)
        return sub

    def unsubscribe(self, request_id: UUID, sub: Subscription) -> None:
        subs = self._subs.get(request_id)
        if subs is None:
            return
        subs.discard(sub)
        if not subs:
            self._subs.pop(request_id, None)

    def replay(self, request_id: UUID) -> tuple[dict, ...]:
        entry = self._recent.get(request_id)
        return tuple(entry[1]) if entry else ()

    # --- публикация ---------------------------------------------------------

    async def publish(self, request_id: UUID, payload: dict) -> None:
        """Вызывается из Kafka-pump и pg-listener'а. Не блокируется ни на чём."""
        self._remember(request_id, payload)
        for sub in tuple(self._subs.get(request_id, ())):
            sub.offer(payload)

    def _remember(self, request_id: UUID, payload: dict) -> None:
        entry = self._recent.get(request_id)
        events = entry[1] if entry else deque(maxlen=BUFFER_MAX_EVENTS)
        events.append(payload)
        self._recent[request_id] = (time.monotonic(), events)
        self._recent.move_to_end(request_id)
        self._prune()

    def _prune(self) -> None:
        """Выкидываем с головы (она же самая старая по обращению): TTL, затем размер."""
        now = time.monotonic()
        while self._recent:
            oldest_id, (touched, _) = next(iter(self._recent.items()))
            if (
                now - touched < BUFFER_TTL_S
                and len(self._recent) <= BUFFER_MAX_REQUESTS
            ):
                break
            self._recent.pop(oldest_id, None)


dispatcher = WSDispatcher()
