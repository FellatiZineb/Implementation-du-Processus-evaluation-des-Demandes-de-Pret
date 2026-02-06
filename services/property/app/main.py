import os
import logging
import random
import threading

from fastapi import FastAPI

from shared.constants import EVENT_LOAN_CREATED, EVENT_PROPERTY_EVALUATED
from shared.schemas import EventEnvelope, LoanCreatedPayload, PropertyEvaluatedPayload
from shared.messaging import consume_events, publish_event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("property")

AMQP_URL = os.getenv("AMQP_URL")

app = FastAPI(title="Property Service")


@app.get("/health")
def health():
    return {"status": "ok"}


def handle_loan_created(event: dict) -> None:
    envelope = EventEnvelope(**event)

    # payload complet attendu (loan_id, client_id, amount, property_id)
    loan_payload = LoanCreatedPayload(**envelope.payload)

    # Exemple: évaluation random (à remplacer par ta logique)
    property_value = float(random.randint(50_000, 500_000))
    property_ok = property_value >= loan_payload.amount * 1.2

    result = PropertyEvaluatedPayload(
        loan_id=loan_payload.loan_id,
        property_value=property_value,
        property_ok=property_ok,
    )

    out_event = EventEnvelope(
        event_type=EVENT_PROPERTY_EVALUATED,
        correlation_id=loan_payload.loan_id,  # UUID
        payload=result.model_dump(mode="json"),
    )

    publish_event(AMQP_URL, EVENT_PROPERTY_EVALUATED, out_event.model_dump(mode="json"))
    logger.info(
        "Property evaluated loan_id=%s value=%s ok=%s",
        loan_payload.loan_id,
        property_value,
        property_ok,
    )


@app.on_event("startup")
def startup():
    thread = threading.Thread(
        target=consume_events,
        args=(AMQP_URL, "q.property", [EVENT_LOAN_CREATED], handle_loan_created),
        daemon=True,
    )
    thread.start()
