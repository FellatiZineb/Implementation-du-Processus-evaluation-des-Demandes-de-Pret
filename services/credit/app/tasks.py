import random
import time
import logging

from shared.messaging import publish_event
from shared.schemas import EventEnvelope, CreditCheckedPayload
from shared.constants import EVENT_CREDIT_CHECKED
from .celery_app import celery_app
import os

logger = logging.getLogger("credit-task")

AMQP_URL = os.getenv(
    "AMQP_URL",
    "amqp://guest:guest@localhost:5672/"
)

@celery_app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={"max_retries": 3})
def check_credit_task(self, loan_id: str):
    logger.info("Starting credit check for loan_id=%s", loan_id)

    time.sleep(5)  # simulation tâche longue

    credit_score = random.randint(300, 900)
    credit_ok = credit_score >= 600

    payload = CreditCheckedPayload(
        loan_id=loan_id,
        credit_score=credit_score,
        credit_ok=credit_ok,
    )

    event = EventEnvelope(
        event_type=EVENT_CREDIT_CHECKED,
        correlation_id=loan_id,
        payload=payload.model_dump(),
    )

    publish_event(
        AMQP_URL,
        EVENT_CREDIT_CHECKED,
        event.model_dump(),
    )

    logger.info("Credit task finished loan_id=%s", loan_id)
