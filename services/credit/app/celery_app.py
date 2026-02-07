from celery import Celery
import os

AMQP_URL = os.getenv(
    "AMQP_URL",
    "amqp://guest:guest@localhost:5672/"
)

celery_app = Celery(
    "credit_tasks",
    broker=AMQP_URL,
    backend="rpc://",
    include=["services.credit.app.tasks"]
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
)
