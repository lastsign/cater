import asyncio
from contextlib import asynccontextmanager
from typing import Annotated
from fastapi import Depends, FastAPI, WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
from src.db import get_db
from src.models.content import Content
from src.realtime.listener import run_listener
from src.realtime.dispatcher import dispatcher


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop = asyncio.Event()
    task = asyncio.create_task(run_listener(stop))
    yield
    stop.set()
    task.cancel()


app = FastAPI(lifespan=lifespan)


@app.WebSocket("/ws/requests/{requests_id}")
async def ws_request(ws: WebSocket, request_id: UUID):
    await ws.accept()
    await dispatcher.subscribe(request_id, ws)
    try:
        snapshot = await load_request_snapshot(request_id)
        await ws.send_json(snapshot)
        while True:
            await ws.recieve_text()
    except WebSocketDisconnect:
        pass
    finally:
        await dispatcher.unsubscribe(request_id, ws)


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
def index(url: str):
    from src.celery_service.tasks import index_url_pipeline
    async_result = index_url_pipeline(url)
    return {"task_id": async_result.id}

@app.get("/index/{task_id}")
async def status(task_id: str, db: Annotated[AsyncSession, Depends(get_db)]):
    from src.celery_service.celery_conn import app as celery_app
    from celery.result import AsyncResult

    res = AsyncResult(task_id, app=celery_app)

    # doc_id протекает по цепочке как результат первой задачи (fetch);
    # дойдём до корня и достанем его (save_text может вернуть кортеж (doc_id, is_new))
    root = res
    while root.parent is not None:
        root = root.parent
    doc_id = root.result
    if isinstance(doc_id, (list, tuple)):
        doc_id = doc_id[0]

    content = await db.get(Content, UUID(doc_id)) if doc_id else None
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
