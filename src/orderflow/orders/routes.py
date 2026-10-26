from fastapi import APIRouter, HTTPException, Response

from orderflow.db import Pool
from orderflow.orders import service
from orderflow.orders.models import OrderIn, OrderOut

router = APIRouter(prefix="/orders", tags=["orders"])


@router.post(
    "",
    status_code=201,
    responses={
        200: {
            "model": OrderOut,
            "description": "This order_ref was accepted before: the stored order, counted once",
        },
        422: {"description": "Invalid order, or a sku with no product"},
    },
)
def create_order(order: OrderIn, response: Response, pool: Pool) -> OrderOut:
    """Accept an order. Safe to retry: an order_ref is only ever counted once."""
    with pool.connection() as conn:
        try:
            accepted, created = service.create_order(conn, order)
        except service.UnknownSkus as exc:
            raise HTTPException(422, f"unknown sku(s): {', '.join(exc.skus)}") from exc
    if not created:
        response.status_code = 200
    return accepted


@router.get("/{order_ref}", responses={404: {"description": "No such order"}})
def get_order(order_ref: str, pool: Pool) -> OrderOut:
    with pool.connection() as conn:
        try:
            return service.get_order(conn, order_ref)
        except service.OrderNotFound as exc:
            raise HTTPException(404, f"no order {order_ref!r}") from exc
