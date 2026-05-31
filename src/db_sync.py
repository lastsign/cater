import json
import os
from functools import partial
from typing import Iterator

from pydantic_core import to_jsonable_python
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session, sessionmaker


def build_sync_url() -> URL:
    return URL.create(
        "postgresql+psycopg",
        username=os.getenv("POSTGRES_USER", "babufrik"),
        password=os.getenv("POSTGRES_PASSWORD", "password"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "babufrik"),
    )


engine_sync = create_engine(
    build_sync_url(),
    json_serializer=partial(json.dumps, default=to_jsonable_python, ensure_ascii=False),
    echo=False,
    pool_pre_ping=True,
)

SessionLocalSync = sessionmaker(bind=engine_sync, autoflush=False, autocommit=False)


# def get_db_sync() -> Iterator[Session]:
#     db = SessionLocalSync()
#     try:
#         yield db
#     finally:
#         db.close()

from contextlib import contextmanager
from typing import Iterator

@contextmanager
def get_db_sync() -> Iterator[Session]:
    with SessionLocalSync() as db:
        yield db
