import random
import logging
import time

from shared.schemas import CreditCheckedPayload, EventEnvelope
from shared.messaging import publish_event
from shared.constants import (
    EVENT_CREDIT_CHECKED,
    EVENT_CREDIT_FAILED,
)

from .celery_app import celery_app
import os

logger = logging.getLogger("credit-tasks")
AMQP_URL = os.getenv("AMQP_URL")


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=5, retry_kwargs={"max_retries": 3})
def check_credit_task(self, loan_id: str):
    logger.info("Checking credit | loan_id=%s", loan_id)

    time.sleep(4)

    credit_score = random.randint(300, 900)

    if credit_score < 600:
        # ❌ ECHEC
        event = EventEnvelope(
            event_type=EVENT_CREDIT_FAILED,
            correlation_id=loan_id,
            payload={"loan_id": loan_id, "reason": "low credit score"},
        )
        publish_event(AMQP_URL, EVENT_CREDIT_FAILED, event.model_dump())
        logger.warning("Credit FAILED | loan_id=%s", loan_id)
        return

    # ✅ SUCCÈS
    payload = CreditCheckedPayload(
        loan_id=loan_id,
        credit_score=credit_score,
        credit_ok=True,
    )

    event = EventEnvelope(
        event_type=EVENT_CREDIT_CHECKED,
        correlation_id=loan_id,
        payload=payload.model_dump(mode="json"),
    )

    publish_event(AMQP_URL, EVENT_CREDIT_CHECKED, event.model_dump())
    logger.info("Credit OK | loan_id=%s", loan_id)
