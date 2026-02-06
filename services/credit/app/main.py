import os
import threading
import random
import logging

from fastapi import FastAPI

from shared.messaging import consume_events, publish_event
from shared.schemas import EventEnvelope, CreditCheckedPayload
from shared.constants import EVENT_LOAN_CREATED, EVENT_CREDIT_CHECKED

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("credit-service")

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")

app = FastAPI(title="Credit Service")


@app.get("/health")
def health():
    return {"status": "credit service running"}


def handle_loan_created(event: dict):
    envelope = EventEnvelope(**event)
    payload = envelope.payload

    loan_id = payload["loan_id"]

    # simulation score crédit
    credit_score = random.randint(300, 900)
    credit_ok = credit_score >= 600

    result = CreditCheckedPayload(
        loan_id=loan_id,
        credit_score=credit_score,
        credit_ok=credit_ok,
    )

    outgoing_event = EventEnvelope(
        event_type=EVENT_CREDIT_CHECKED,
        correlation_id=loan_id,
        payload=result.model_dump(),
    )

    publish_event(
        AMQP_URL,
        EVENT_CREDIT_CHECKED,
        outgoing_event.model_dump(),
    )

    logger.info(
        f"Credit checked | loan_id={loan_id} | score={credit_score} | ok={credit_ok}"
    )


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
