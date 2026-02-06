import os
import threading
import random
import logging

from fastapi import FastAPI

from shared.messaging import consume_events, publish_event
from shared.schemas import EventEnvelope, PropertyEvaluatedPayload
from shared.constants import EVENT_LOAN_CREATED, EVENT_PROPERTY_EVALUATED

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("property-service")

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")

app = FastAPI(title="Property Service")


@app.get("/health")
def health():
    return {"status": "property service running"}


def handle_loan_created(event: dict):
    envelope = EventEnvelope(**event)
    payload = envelope.payload

    loan_id = payload["loan_id"]

    # simulation estimation immobilière
    property_value = random.randint(100_000, 500_000)
    property_ok = property_value >= payload["amount"]

    result = PropertyEvaluatedPayload(
        loan_id=loan_id,
        property_value=property_value,
        property_ok=property_ok,
    )

    outgoing_event = EventEnvelope(
        event_type=EVENT_PROPERTY_EVALUATED,
        correlation_id=loan_id,
        payload=result.model_dump(),
    )

    publish_event(
        AMQP_URL,
        EVENT_PROPERTY_EVALUATED,
        outgoing_event.model_dump(),
    )

    logger.info(
        f"Property evaluated | loan_id={loan_id} | value={property_value} | ok={property_ok}"
    )


@app.on_event("startup")
def startup_event():
    thread = threading.Thread(
        target=consume_events,
        args=(
            AMQP_URL,
            "q.property",
            [EVENT_LOAN_CREATED],
            handle_loan_created,
        ),
        daemon=True,
    )
    thread.start()
