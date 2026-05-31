from celery import Celery

app = Celery(
    "hello",
    broker="redis://:your_secure_password@localhost:6379/0",
    backend="redis://:your_secure_password@localhost:6379/0"
)