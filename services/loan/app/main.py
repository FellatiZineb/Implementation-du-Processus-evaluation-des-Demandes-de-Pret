from fastapi import FastAPI, HTTPException
from uuid import UUID, uuid4
import os
import logging

from shared.schemas import LoanCreateRequest, EventEnvelope, LoanCreatedPayload
from shared.constants import EVENT_LOAN_CREATED
from shared.messaging import publish_event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("loan")

AMQP_URL = os.getenv("AMQP_URL")

app = FastAPI(title="Loan Service")

# stockage simple pour exercice 1
loans = {}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/loans")
def create_loan(req: LoanCreateRequest):
    loan_id = uuid4()

    loans[loan_id] = {
        "loan_id": loan_id,
        "status": "CREATED",
        "client_id": req.client_id,
        "amount": req.amount,
        "property_id": req.property_id,
    }

    payload = LoanCreatedPayload(
        loan_id=loan_id,
        client_id=req.client_id,
        amount=req.amount,
        property_id=req.property_id,
    )

    event = EventEnvelope(
        event_type=EVENT_LOAN_CREATED,
        correlation_id=loan_id,
        payload=payload.model_dump(),
    )

    publish_event(AMQP_URL, EVENT_LOAN_CREATED, event.model_dump())

    logger.info("Loan created loan_id=%s", loan_id)
    return {"loan_id": loan_id}


@app.get("/loans/{loan_id}")
def get_loan(loan_id: UUID):
    loan = loans.get(loan_id)
    if not loan:
        raise HTTPException(status_code=404, detail="Loan not found")
    return loan
