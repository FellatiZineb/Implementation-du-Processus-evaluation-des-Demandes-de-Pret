import os
import threading
import logging

from fastapi import FastAPI
from shared.constants import EVENT_LOAN_CREATED
from shared.schemas import EventEnvelope, LoanCreatedPayload
from shared.messaging import consume_events
from .tasks import evaluate_property_task

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("property")

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")

app = FastAPI(title="Property Service")

@app.get("/health")
def health():
    return {"status": "property service running"}

def handle_loan_created(event: dict):
    envelope = EventEnvelope(**event)
    payload = LoanCreatedPayload(**envelope.payload)

    evaluate_property_task.delay(
        str(payload.loan_id),
        payload.amount
    )

    logger.info("Property task queued | loan_id=%s", payload.loan_id)


@app.on_event("startup")
def startup():
    thread = threading.Thread(
        target=consume_events,
        args=(AMQP_URL, "q.property", [EVENT_LOAN_CREATED], handle_loan_created),
        daemon=True,
    )
    thread.start()
