from src.celery_service.tasks import add


result = add.delay(4, 4)
print(f"Task ID: {result.id}")
