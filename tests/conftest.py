import os
from collections import Counter
from collections.abc import Callable, Iterator

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg_pool import ConnectionPool

from orderflow.api import create_app
from orderflow.db import Conn, create_pool
from orderflow.migrate import migrate
from orderflow.stock import service as stock
from orderflow.worker import CONSUMERS

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://orderflow:orderflow@localhost:55432/orderflow_test"
)

PRODUCTS = [("BAN-001", "Bananas 1kg", 199, 50), ("APL-003", "Apples 1.5kg", 349, 40)]


@pytest.fixture(scope="session")
def database_url() -> str:
    """A fresh, migrated database next to the app's one."""
    name = conninfo_to_dict(TEST_DATABASE_URL)["dbname"]
    with psycopg.connect(
        make_conninfo(TEST_DATABASE_URL, dbname="postgres"), autocommit=True
    ) as admin:
        admin.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
        )
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    with psycopg.connect(TEST_DATABASE_URL) as conn:
        migrate(conn)
    return TEST_DATABASE_URL


@pytest.fixture(scope="session")
def pool(database_url: str) -> Iterator[ConnectionPool[Conn]]:
    with create_pool(database_url, max_size=20) as pool:
        yield pool


@pytest.fixture(scope="session")
def client(database_url: str) -> Iterator[TestClient]:
    with TestClient(create_app(database_url)) as client:
        yield client


@pytest.fixture(autouse=True)
def fresh_data(pool: ConnectionPool[Conn]) -> None:
    with pool.connection() as conn:
        conn.execute(
            "TRUNCATE stock.products, stock.movements, orders.orders, orders.order_items,"
            " eventlog.events RESTART IDENTITY"
        )
        conn.execute("UPDATE eventlog.consumer_offsets SET last_event_id = 0")
        for sku, name, price_cents, opening_stock in PRODUCTS:
            stock.upsert_product(conn, sku, name, price_cents, opening_stock)


@pytest.fixture
def run_worker(pool: ConnectionPool[Conn]) -> Callable[[], Counter[str]]:
    """Runs the worker's consumers until nothing is left; returns how many events each processed."""

    def run() -> Counter[str]:
        processed: Counter[str] = Counter()
        while True:
            busy = False
            for name, consume in CONSUMERS.items():
                with pool.connection() as conn:
                    events = consume(conn, 2)  # small batches, to exercise paging
                processed[name] += len(events)
                busy = busy or bool(events)
            if not busy:
                return processed

    return run
