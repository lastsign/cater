from __future__ import annotations

import json
import os
from functools import partial
from typing import AsyncIterator

from pydantic_core import to_jsonable_python
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def build_async_url() -> URL:
    return URL.create(
        "postgresql+asyncpg",
        username=os.getenv("POSTGRES_USER", "babufrik"),
        password=os.getenv("POSTGRES_PASSWORD", "password"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "babufrik"),
    )


engine = create_async_engine(
    build_async_url(),
    json_serializer=partial(json.dumps, default=to_jsonable_python, ensure_ascii=False),
    echo=False,
    pool_pre_ping=True,
)

SessionLocal = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    autoflush=False,
)


async def get_db() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session
