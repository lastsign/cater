"""WS endpoint for request status: a snapshot plus the live stream from index.events.

The order of operations in stream_request is the only thing here that truly matters:

    accept -> subscribe -> snapshot -> live

Subscribing BEFORE reading the snapshot: an event can slip through between the SELECT
and the subscription and would be lost. The flip side is duplicates (an event present
both in the snapshot and in the queue), hence dedup by event_id rather than by order.
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

# Idle time after which we send a ping: proxies cut WS connections without traffic
# (nginx does it after 60s).
HEARTBEAT_S = float(os.getenv("WS_HEARTBEAT_S", "25"))
# How many event_ids to remember for snapshot/replay/live dedup.
SEEN_MAX = 256


async def _wait_disconnect(ws: WebSocket) -> None:
    """Its only job is to notice that the client is gone.

    Incoming messages have to be read anyway: without a receive, Starlette never sees
    the close frame and the connection hangs around together with the subscription.
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
            # The event-waiting task is kept between iterations: recreating it after
            # every heartbeat is not allowed - the cancel would swallow an event that
            # has already been taken out of the queue.
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
            pass  # already closed by the client
