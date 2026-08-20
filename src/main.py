import asyncio
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, WebSocket
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import get_db
from src.kafka_service.async_client import producer, run_event_pump
from src.kafka_service.projector import run_status_projector
from src.models.content import Content
from src.realtime.dispatcher import dispatcher
from src.realtime.ws import stream_request
from src.storage import create_request


@asynccontextmanager
async def lifespan(app: FastAPI):
    await producer.start()
    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(run_event_pump(stop, dispatcher.publish)),
        asyncio.create_task(run_status_projector(stop)),
    ]
    yield
    stop.set()
    for t in tasks:
        t.cancel()
    await producer.stop()


app = FastAPI(lifespan=lifespan)


@app.websocket("/ws/requests/{request_id}")
async def ws_request(ws: WebSocket, request_id: UUID):
    await stream_request(ws, request_id)


class FixedContentQueryChecker:
    def __init__(self, fixed_content: str):
        self.fixed_content = fixed_content

    def __call__(self, q: str = ""):
        if q:
            return self.fixed_content in q
        return False


checker = FixedContentQueryChecker("bar")


@app.get("/query-checker")
async def read_query_check(fixed_content_included: Annotated[bool, Depends(checker)]):
    return {"fixed_content_in_query": fixed_content_included}


@app.post("/index")
async def index(url: str):
    request_id = await producer.submit_index_request(url=url)
    await create_request(request_id, url)
    return {"request_id": request_id}


@app.get("/index/{task_id}")
async def status(task_id: str, db: Annotated[AsyncSession, Depends(get_db)]):
    from celery.result import AsyncResult

    from src.celery_service.celery_conn import app as celery_app

    res = AsyncResult(task_id, app=celery_app)

    # doc_id flows down the chain as the result of the first task (fetch).
    # AsyncResult.parent is NOT restored from the backend for a bare task_id (it is
    # always None), so we walk the parent_id stored in meta up to the root.
    backend = celery_app.backend
    root_meta = backend.get_task_meta(task_id)
    while root_meta.get("parent_id"):
        root_meta = backend.get_task_meta(root_meta["parent_id"])
    doc_id = root_meta.get("result")
    if isinstance(doc_id, (list, tuple)):
        doc_id = doc_id[0]

    content = None
    if isinstance(doc_id, str):
        try:
            content = await db.get(Content, UUID(doc_id))
        except ValueError:
            content = None
    if content is None:
        return {"task_id": task_id, "state": res.state, "status": None}

    return {
        "task_id": task_id,
        "state": res.state,
        "doc_id": str(content.id),
        "status": content.status,
        "title": content.title,
        "source_url": content.source_url,
    }
