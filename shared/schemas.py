# shared/schemas.py

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class EventEnvelope(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    occurred_at: datetime = Field(default_factory=utc_now)
    correlation_id: UUID
    payload: Dict[str, Any]


class LoanCreateRequest(BaseModel):
    client_id: str = Field(min_length=1)
    amount: float = Field(gt=0)
    property_id: str = Field(min_length=1)


class LoanCreatedPayload(BaseModel):
    loan_id: UUID
    client_id: str
    amount: float
    property_id: str


class CreditCheckedPayload(BaseModel):
    loan_id: UUID
    credit_score: int = Field(ge=0, le=1000)
    credit_ok: bool


class PropertyEvaluatedPayload(BaseModel):
    loan_id: UUID
    property_value: float = Field(gt=0)
    property_ok: bool


DecisionValue = Literal["APPROVED", "REJECTED"]


class DecisionMadePayload(BaseModel):
    loan_id: UUID
    decision: DecisionValue
    reasons: List[str] = Field(default_factory=list)
