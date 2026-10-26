"""HTTP API: order intake, stock reads and the order feed.

    uvicorn orderflow.api:create_app --factory

Stateless: everything lives in Postgres, so any number of copies can run side by side.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from orderflow.config import Settings
from orderflow.db import Pool, create_pool
from orderflow.orders.feed import router as feed_router
from orderflow.orders.routes import router as orders_router
from orderflow.stock.routes import router as stock_router


def create_app(database_url: str | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        with create_pool(database_url or Settings.from_env().database_url) as pool:
            app.state.pool = pool
            yield

    app = FastAPI(title="Orders & Stock", lifespan=lifespan)
    app.include_router(orders_router)
    app.include_router(stock_router)
    app.include_router(feed_router)

    @app.exception_handler(psycopg.OperationalError)
    async def database_unavailable(request: Request, exc: Exception) -> JSONResponse:
        # Clients can safely retry: orders are idempotent on order_ref.
        return JSONResponse(
            {"detail": "database unavailable, please retry"},
            status_code=503,
            headers={"Retry-After": "1"},
        )

    @app.get("/healthz", tags=["ops"])
    def healthz(pool: Pool) -> dict[str, str]:
        with pool.connection() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok"}

    return app
