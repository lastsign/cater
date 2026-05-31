import asyncio, json, asyncpg, logging
from uuid import UUID
from src.realtime.dispatcher import dispatcher
from src.config import settings

log = logging.getLogger(__name__)


async def _on_notife(conn, pid, channel, payload):
    try:
        data = json.loads(payload)
        request_id = UUID(data["request_id"])
    except Exception:
        log.exception("bad notify payload: %s", payload)
        return
    await dispatcher.publish(request_id, data)


async def run_listener(stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            conn = await asyncpg.connect(settings.POSTGRES_DSN)
            await conn.add_listener("request_update", _on_notife)
            log.info("pg listener connected")
            while not stop_event.is_set():
                await asyncio.sleep(30)
                await conn.execute("SELECT 1")
        except Exception:
            log.exception("listener chashed, reconnecting in 2s")
            await asyncio.sleep(2)
        finally:
            try:
                await conn.close()
            except Exception:
                pass
