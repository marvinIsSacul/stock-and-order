from collections.abc import Iterable
from typing import Any

from orderflow import eventlog
from orderflow.db import Conn
from orderflow.stock.models import Freshness, Product

CONSUMER = "stock"
SUBSCRIBES_TO = [eventlog.ORDER_ACCEPTED]


def get_prices(conn: Conn, skus: Iterable[str]) -> dict[str, int]:
    """Current prices of the given skus. Unknown skus are left out."""
    rows = conn.execute(
        "SELECT sku, price_cents FROM stock.products WHERE sku = ANY(%s)", (list(skus),)
    ).fetchall()
    return {row["sku"]: row["price_cents"] for row in rows}


def process_next_batch(conn: Conn, batch_size: int) -> list[eventlog.Event]:
    """Take the next accepted orders off stock, in one transaction (see eventlog.consume)."""
    return eventlog.consume(conn, CONSUMER, SUBSCRIBES_TO, _reserve_stock, batch_size)


def _reserve_stock(conn: Conn, event: eventlog.Event) -> list[eventlog.NewEvent]:
    """Take an accepted order's items off stock, once, and announce it with stock.reserved."""
    reserved: list[dict[str, Any]] = []
    for item in event.data["items"]:
        # The movement's key (event_id, sku) makes this a no-op for an event already applied,
        # even if the offset is ever rewound.
        recorded = conn.execute(
            """
            INSERT INTO stock.movements (event_id, sku, delta, order_ref) VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING sku
            """,
            (event.id, item["sku"], -item["qty"], event.key),
        ).fetchone()
        if recorded:
            conn.execute(
                "UPDATE stock.products SET stock = stock - %s, updated_at = now() WHERE sku = %s",
                (item["qty"], item["sku"]),
            )
            reserved.append({"sku": item["sku"], "qty": item["qty"]})
    if not reserved:
        return []
    data = {"order_ref": event.key, "items": reserved}
    return [eventlog.NewEvent(eventlog.STOCK_RESERVED, event.key, data)]


def read_stock(conn: Conn, sku: str | None = None) -> tuple[list[Product], Freshness]:
    """Products and their stock (all, or one sku), and how far behind accepted orders they are."""
    with conn.transaction():
        # One snapshot for both queries, so the stock levels and the freshness agree.
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        applied_up_to, pending = eventlog.position(conn, CONSUMER, SUBSCRIBES_TO)
        rows = conn.execute(
            "SELECT sku, name, price_cents, stock, updated_at FROM stock.products"
            " WHERE %(sku)s::text IS NULL OR sku = %(sku)s ORDER BY sku",
            {"sku": sku},
        ).fetchall()
    freshness = Freshness(applied_up_to_event=applied_up_to, pending_events=pending)
    return [Product(**row) for row in rows], freshness


def upsert_product(conn: Conn, sku: str, name: str, price_cents: int, opening_stock: int) -> None:
    """Add a product, or update its name and price. Stock is only set when the product is new."""
    conn.execute(
        """
        INSERT INTO stock.products (sku, name, price_cents, stock) VALUES (%s, %s, %s, %s)
        ON CONFLICT (sku) DO UPDATE
        SET name = EXCLUDED.name, price_cents = EXCLUDED.price_cents, updated_at = now()
        """,
        (sku, name, price_cents, opening_stock),
    )
