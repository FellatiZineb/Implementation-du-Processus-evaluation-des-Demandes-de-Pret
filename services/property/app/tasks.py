import os
import logging

from shared.messaging import publish_event
from shared.constants import EVENT_PROPERTY_EVALUATED, EVENT_PROPERTY_FAILED
from .celery_app import celery_app

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/%2F")
logger = logging.getLogger("property-worker")

@celery_app.task(name="evaluate_property_task")
def evaluate_property_task(correlation_id: str, loan_id: str, amount: float):
    try:
        result = {
            "loan_id": loan_id,
            "property_value": float(amount) * 1.2,
            "property_ok": True,
        }

        event_dict = {
            "event_type": EVENT_PROPERTY_EVALUATED,
            "correlation_id": correlation_id,
            "payload": result,
        }

        logger.info("Publishing property.evaluated envelope: %s", event_dict)
        publish_event(AMQP_URL, EVENT_PROPERTY_EVALUATED, event_dict)
        return result

    except Exception as exc:
        event_dict = {
            "event_type": EVENT_PROPERTY_FAILED,
            "correlation_id": correlation_id,
            "payload": {"loan_id": loan_id, "reason": str(exc)},
        }

        logger.info("Publishing property.failed envelope: %s", event_dict)
        publish_event(AMQP_URL, EVENT_PROPERTY_FAILED, event_dict)
        raise
