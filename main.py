import asyncio
import time
from fastapi import APIRouter


router = APIRouter()


@router.get("/ping-1")
async def ping_1():
    time.sleep(10)
    
    return {"pong": True}

@router.get("/ping-2")
def ping_2():
    time.sleep(10)

    return {"pong": True}

@router.get("/ping-3")
async def ping_3():
    await asyncio.sleep(10)

    return {"pong": True}