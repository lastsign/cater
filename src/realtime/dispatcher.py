import asyncio
from collections import defaultdict
from uuid import UUID
from fastapi import WebSocket


class WSDispatcher:
    def __init__(self):
        self._subs: dict[UUID, set[WebSocket]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def subscribe(self, request_id: UUID, ws: WebSocket):
        async with self._lock:
            self._subs[request_id].add(ws)
    
    async def unsubscribe(self, request_id: UUID, ws: WebSocket):
        async with self._lock:
            self._subs[request_id].discard(ws)
            if not self._subs[request_id]:
                del self._subs[request_id]

    async def publish(self, request_id: UUID, payload: dict):
        subs = list(self._subs.get(request_id, ()))
        for ws in subs:
            try:
                await ws.send_json(payload)
            except Exception:
                await self.unsubscribe(request_id, ws)


dispatcher = WSDispatcher()
