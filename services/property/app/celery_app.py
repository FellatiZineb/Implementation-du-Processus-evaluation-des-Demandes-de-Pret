from celery import Celery
import os

AMQP_URL = os.getenv(
    "AMQP_URL",
    "amqp://guest:guest@localhost:5672//"
)

celery_app = Celery(
    "property",
    broker=AMQP_URL,
    backend="rpc://",
    include=["services.property.app.tasks"]  
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)
