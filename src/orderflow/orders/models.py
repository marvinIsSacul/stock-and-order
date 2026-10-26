from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator

# Sanity limits on what a client can send, not business rules. They keep bad input away from the
# database (a huge qty would overflow the integer column and fail as a 500) and stop typos such
# as 1,000,000 bananas. Real limits, like "max 10 per customer" on a promotion, would come from
# the business.
MAX_QTY_PER_LINE = 1000
MAX_LINES_PER_ORDER = 100
MAX_ID_LENGTH = 64  # order_ref, customer_id and sku

# Only characters that are safe in a URL path, since order_ref appears in GET /orders/{order_ref}.
ORDER_REF_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"


class OrderItemIn(BaseModel):
    sku: str = Field(min_length=1, max_length=MAX_ID_LENGTH, examples=["BAN-001"])
    # At least 1: a negative quantity would add stock instead of taking it off.
    qty: int = Field(ge=1, le=MAX_QTY_PER_LINE, examples=[2])


class OrderIn(BaseModel):
    order_ref: str = Field(
        min_length=1, max_length=MAX_ID_LENGTH, pattern=ORDER_REF_PATTERN, examples=["web-100045"]
    )
    customer_id: str = Field(min_length=1, max_length=MAX_ID_LENGTH, examples=["cust-42"])
    items: list[OrderItemIn] = Field(min_length=1, max_length=MAX_LINES_PER_ORDER)

    @field_validator("items")
    @classmethod
    def one_line_per_sku(cls, items: list[OrderItemIn]) -> list[OrderItemIn]:
        if len({item.sku for item in items}) != len(items):
            raise ValueError("each sku may appear only once per order")
        return items


class OrderItemOut(BaseModel):
    sku: str
    qty: int
    unit_price_cents: int


class OrderOut(BaseModel):
    order_ref: str
    customer_id: str
    status: Literal["accepted", "confirmed"] = Field(
        description="accepted: stored, stock not reserved yet. confirmed: stock reserved."
    )
    total_cents: int
    items: list[OrderItemOut]
    created_at: datetime
