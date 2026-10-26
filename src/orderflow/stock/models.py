from datetime import datetime

from pydantic import BaseModel, Field

APPLIED_UP_TO = "Last accepted order (event id) reflected in the stock levels"
PENDING = "Accepted orders not reflected in the stock levels yet; 0 means up to date"


class Product(BaseModel):
    sku: str
    name: str
    price_cents: int
    stock: int = Field(description="Negative when more was ordered than was in stock")
    updated_at: datetime


class ProductStock(Product):
    applied_up_to_event: int = Field(description=APPLIED_UP_TO)
    pending_events: int = Field(description=PENDING)


class Freshness(BaseModel):
    applied_up_to_event: int = Field(description=APPLIED_UP_TO)
    pending_events: int = Field(description=PENDING)


class StockList(Freshness):
    products: list[Product]
