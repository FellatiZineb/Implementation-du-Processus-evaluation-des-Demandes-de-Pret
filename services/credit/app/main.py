import os
import threading
import logging

from fastapi import FastAPI

from .tasks import check_credit_task  # Celery task
from shared.messaging import consume_events
from shared.schemas import EventEnvelope
from shared.constants import EVENT_LOAN_CREATED

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("credit-service")

AMQP_URL = os.getenv(
    "AMQP_URL",
    "amqp://guest:guest@localhost:5672/"
)

app = FastAPI(title="Credit Service")


@app.get("/health")
def health():
    return {"status": "credit service running"}


def handle_loan_created(event: dict):
    envelope = EventEnvelope(**event)
    payload = envelope.payload

    loan_id = payload["loan_id"]

    #  délégation au worker Celery
    check_credit_task.delay(str(loan_id))

    logger.info("Credit task queued | loan_id=%s", loan_id)


@app.on_event("startup")
def startup_event():
    thread = threading.Thread(
        target=consume_events,
        args=(
            AMQP_URL,
            "credit-queue",
            [EVENT_LOAN_CREATED],
            handle_loan_created,
        ),
        daemon=True,
    )
    thread.start()
