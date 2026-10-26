"""The order feed: how other teams learn about accepted orders.

Consumers poll GET /events?after=<id of the last event they processed> and keep their own
position, the way a Kafka consumer keeps an offset. Only order.accepted is published here;
events the components exchange among themselves (stock.reserved) stay internal.
"""

from dataclasses import asdict
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from orderflow import eventlog
from orderflow.db import Pool

router = APIRouter(tags=["feed"])


class FeedEvent(BaseModel):
    id: int = Field(description="Position in the feed")
    type: str = Field(examples=[eventlog.ORDER_ACCEPTED])
    key: str = Field(description="The order_ref this event is about")
    data: dict[str, Any] = Field(description="The accepted order, as returned by GET /orders/{ref}")
    created_at: datetime


class FeedPage(BaseModel):
    events: list[FeedEvent]
    next_after: int = Field(description="Pass as `after` to get the next page")


@router.get("/events")
def read_events(
    pool: Pool,
    after: Annotated[int, Query(ge=0, description="Id of the last event you processed")] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> FeedPage:
    """order.accepted events, in the order they were committed. Paging by id never skips one.

    Delivery is at-least-once: store your position after processing an event, so a crash between
    the two means you see that event again. Use the event id to ignore repeats.
    """
    with pool.connection() as conn:
        events = eventlog.read(conn, after=after, limit=limit, types=[eventlog.ORDER_ACCEPTED])
    return FeedPage(
        events=[FeedEvent(**asdict(event)) for event in events],
        next_after=events[-1].id if events else after,
    )
