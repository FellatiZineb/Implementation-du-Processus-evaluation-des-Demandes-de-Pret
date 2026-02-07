import random
import time
import logging
from shared.schemas import PropertyEvaluatedPayload

from .celery_app import celery_app

logger = logging.getLogger("property-tasks")


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=5, retry_kwargs={"max_retries": 3})
def evaluate_property_task(self, loan_id: str, amount: float):
    logger.info("Evaluating property | loan_id=%s", loan_id)

    # simulation tâche longue
    time.sleep(5)

    property_value = float(random.randint(50_000, 500_000))
    property_ok = property_value >= amount * 1.2

    return PropertyEvaluatedPayload(
        loan_id=loan_id,
        property_value=property_value,
        property_ok=property_ok,
    ).model_dump(mode="json")
