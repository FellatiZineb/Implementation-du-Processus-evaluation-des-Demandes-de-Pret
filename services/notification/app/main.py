import os
import threading
import logging

from fastapi import FastAPI

from shared.messaging import consume_events
from shared.schemas import EventEnvelope, DecisionMadePayload
from shared.constants import EVENT_DECISION_MADE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("notification-service")

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")

app = FastAPI(title="Notification Service")


@app.get("/health")
def health():
    return {"status": "notification service running"}


def handle_decision_made(event: dict):
    envelope = EventEnvelope(**event)
    payload = DecisionMadePayload(**envelope.payload)

    loan_id = payload.loan_id
    decision = payload.decision
    reasons = payload.reasons

    # Simulation notification client
    logger.info(
        f"Notification sent | loan_id={loan_id} | decision={decision} | reasons={reasons}"
    )


@app.on_event("startup")
def startup_event():
    thread = threading.Thread(
        target=consume_events,
        args=(
            AMQP_URL,
            "q.notification",
            [EVENT_DECISION_MADE],
            handle_decision_made,
        ),
        daemon=True,
    )
    thread.start()
