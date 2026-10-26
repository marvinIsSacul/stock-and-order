from fastapi import APIRouter, HTTPException

from orderflow.db import Pool
from orderflow.stock import service
from orderflow.stock.models import ProductStock, StockList

router = APIRouter(prefix="/stock", tags=["stock"])


@router.get("")
def list_stock(pool: Pool) -> StockList:
    """Every product and its current stock."""
    with pool.connection() as conn:
        products, freshness = service.read_stock(conn)
    return StockList(products=products, **freshness.model_dump())


@router.get("/{sku}", responses={404: {"description": "No such product"}})
def get_stock(sku: str, pool: Pool) -> ProductStock:
    """Current stock for a sku.

    Stock follows accepted orders asynchronously: `pending_events` above 0 means some accepted
    orders aren't reflected yet, for example while the worker is down.
    """
    with pool.connection() as conn:
        products, freshness = service.read_stock(conn, sku)
    if not products:
        raise HTTPException(404, f"no product {sku!r}")
    return ProductStock(**products[0].model_dump(), **freshness.model_dump())
