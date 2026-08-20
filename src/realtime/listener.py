import json
import logging
from uuid import UUID

from src.realtime.dispatcher import dispatcher

log = logging.getLogger(__name__)


async def _on_notife(conn, pid, channel, payload):
    try:
        data = json.loads(payload)
        request_id = UUID(data["request_id"])
    except Exception:
        log.exception("bad notify payload: %s", payload)
        return
    await dispatcher.publish(request_id, data)
