"""Fan-out of status events to the WS subscribers of one request_id.

The module rests on two decisions:

1. A subscription is a per-connection queue, not the WebSocket itself. publish never
   waits for a client: a slow or stuck browser loses events but does not slow down the
   Kafka pump - and slowing it down would hold up the whole index.events group.
2. A buffer of recent events per request_id. Tens of milliseconds pass between "HTTP
   returned the request_id" and "the client opened the WS", while fetch finishes in
   hundreds - so the event carrying doc_id would go nowhere. On subscribe the buffer is
   handed over as a replay, so the client gets the doc_id even if it was late.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import OrderedDict, defaultdict, deque
from uuid import UUID

log = logging.getLogger(__name__)

# How long to keep a request's events after the last update, and how many to remember.
BUFFER_TTL_S = float(os.getenv("WS_BUFFER_TTL_S", "900"))
BUFFER_MAX_EVENTS = int(os.getenv("WS_BUFFER_MAX_EVENTS", "32"))
BUFFER_MAX_REQUESTS = int(os.getenv("WS_BUFFER_MAX_REQUESTS", "10000"))
# Depth of a connection's queue. Overflow means the client is not reading; we do not
# drop from the head - we lose the new events and count the losses so they show in logs.
QUEUE_MAXSIZE = int(os.getenv("WS_QUEUE_MAXSIZE", "256"))


class Subscription:
    """Event queue of a single WS connection."""

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
        # request_id -> (time of the last event, the recent events)
        self._recent: OrderedDict[UUID, tuple[float, deque[dict]]] = OrderedDict()

    # --- subscriptions ------------------------------------------------------

    def subscribe(self, request_id: UUID) -> Subscription:
        """Subscribe plus a replay of the buffer. Synchronous: within one loop no lock is needed."""
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

    # --- publishing ---------------------------------------------------------

    async def publish(self, request_id: UUID, payload: dict) -> None:
        """Called from the Kafka pump and the pg listener. Never blocks on anything."""
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
        """Evict from the head (the least recently touched): by TTL first, then by size."""
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
