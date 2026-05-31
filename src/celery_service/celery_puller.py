import sys
from celery.result import AsyncResult

from src.celery_service.celery_conn import app

if len(sys.path) > 1:
    task_id = sys.path[1]
else:
    task_id = ""

print(task_id, sys.path)

res = AsyncResult(task_id, app=app)

if res.ready():
    print(f"Task Result: {res.result}")
else:
    print("Task is still processing...")
