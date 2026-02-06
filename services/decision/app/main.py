import os
import threading
import logging

from fastapi import FastAPI

from shared.messaging import consume_events, publish_event
from shared.schemas import (
    EventEnvelope,
    CreditCheckedPayload,
    PropertyEvaluatedPayload,
    DecisionMadePayload,
)
from shared.constants import (
    EVENT_CREDIT_CHECKED,
    EVENT_PROPERTY_EVALUATED,
    EVENT_DECISION_MADE,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("decision-service")

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")

app = FastAPI(title="Decision Service")

# stockage temporaire en mémoire
decision_state = {}


@app.get("/health")
def health():
    return {"status": "decision service running"}


def try_make_decision(loan_id):
    state = decision_state.get(loan_id)
    if not state:
        return

    if "credit" in state and "property" in state:
        credit_ok = state["credit"]["credit_ok"]
        property_ok = state["property"]["property_ok"]

        decision = "APPROVED" if credit_ok and property_ok else "REJECTED"

        reasons = []
        if not credit_ok:
            reasons.append("credit check failed")
        if not property_ok:
            reasons.append("property value insufficient")

        payload = DecisionMadePayload(
            loan_id=loan_id,
            decision=decision,
            reasons=reasons,
        )

        event = EventEnvelope(
            event_type=EVENT_DECISION_MADE,
            correlation_id=loan_id,
            payload=payload.model_dump(),
        )

        publish_event(
            AMQP_URL,
            EVENT_DECISION_MADE,
            event.model_dump(),
        )

        logger.info(
            f"Decision made | loan_id={loan_id} | decision={decision}"
        )

        # nettoyage
        del decision_state[loan_id]


def handle_credit_checked(event: dict):
    envelope = EventEnvelope(**event)
    payload = CreditCheckedPayload(**envelope.payload)

    loan_id = payload.loan_id

    decision_state.setdefault(loan_id, {})["credit"] = payload.model_dump()

    logger.info(f"Credit result received | loan_id={loan_id}")

    try_make_decision(loan_id)


def handle_property_evaluated(event: dict):
    envelope = EventEnvelope(**event)
    payload = PropertyEvaluatedPayload(**envelope.payload)

    loan_id = payload.loan_id

    decision_state.setdefault(loan_id, {})["property"] = payload.model_dump()

    logger.info(f"Property result received | loan_id={loan_id}")

    try_make_decision(loan_id)


@app.on_event("startup")
def startup_event():
    thread_credit = threading.Thread(
        target=consume_events,
        args=(
            AMQP_URL,
            "q.decision.credit",
            [EVENT_CREDIT_CHECKED],
            handle_credit_checked,
        ),
        daemon=True,
    )

    thread_property = threading.Thread(
        target=consume_events,
        args=(
            AMQP_URL,
            "q.decision.property",
            [EVENT_PROPERTY_EVALUATED],
            handle_property_evaluated,
        ),
        daemon=True,
    )

    thread_credit.start()
    thread_property.start()
