"""Order intake, and the order lifecycle: accepted, then confirmed once its stock is reserved."""

from orderflow import eventlog, stock
from orderflow.db import Conn
from orderflow.orders.models import OrderIn, OrderItemOut, OrderOut

CONSUMER = "orders"


class UnknownSkus(Exception):
    def __init__(self, skus: list[str]) -> None:
        super().__init__(f"unknown skus: {', '.join(skus)}")
        self.skus = skus


class OrderNotFound(Exception):
    pass


def create_order(conn: Conn, order: OrderIn) -> tuple[OrderOut, bool]:
    """Accept an order and publish order.accepted. Returns the stored order and whether this
    call created it.

    Only the first submission of an order_ref counts. A later submission of the same order_ref,
    whatever it contains, gets the stored order back with created=False.
    """
    with conn.transaction():
        prices = stock.get_prices(conn, [item.sku for item in order.items])
        unknown = sorted({item.sku for item in order.items} - prices.keys())
        if unknown:
            raise UnknownSkus(unknown)
        items = sorted(
            (
                OrderItemOut(sku=item.sku, qty=item.qty, unit_price_cents=prices[item.sku])
                for item in order.items
            ),
            key=lambda item: item.sku,
        )
        total_cents = sum(item.qty * item.unit_price_cents for item in items)

        # The primary key on order_ref is what stops double counting. If another transaction is
        # inserting the same order_ref right now, this waits for it to commit, then inserts nothing.
        inserted = conn.execute(
            """
            INSERT INTO orders.orders (order_ref, customer_id, status, total_cents)
            VALUES (%s, %s, 'accepted', %s)
            ON CONFLICT (order_ref) DO NOTHING
            RETURNING created_at
            """,
            (order.order_ref, order.customer_id, total_cents),
        ).fetchone()
        if inserted is None:  # accepted before: the first submission is the one that counts
            return get_order(conn, order.order_ref), False

        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO orders.order_items (order_ref, sku, qty, unit_price_cents)"
                " VALUES (%s, %s, %s, %s)",
                [(order.order_ref, item.sku, item.qty, item.unit_price_cents) for item in items],
            )
        accepted = OrderOut(
            order_ref=order.order_ref,
            customer_id=order.customer_id,
            status="accepted",
            total_cents=total_cents,
            items=items,
            created_at=inserted["created_at"],
        )
        eventlog.append(
            conn, eventlog.ORDER_ACCEPTED, order.order_ref, accepted.model_dump(mode="json")
        )
    return accepted, True


def get_order(conn: Conn, order_ref: str) -> OrderOut:
    row = conn.execute(
        """
        SELECT o.order_ref, o.customer_id, o.status, o.total_cents, o.created_at,
               json_agg(json_build_object('sku', i.sku, 'qty', i.qty,
                                          'unit_price_cents', i.unit_price_cents)
                        ORDER BY i.sku) AS items
        FROM orders.orders o JOIN orders.order_items i USING (order_ref)
        WHERE o.order_ref = %s
        GROUP BY o.order_ref
        """,
        (order_ref,),
    ).fetchone()
    if row is None:
        raise OrderNotFound(order_ref)
    return OrderOut.model_validate(row)


def process_next_batch(conn: Conn, batch_size: int) -> list[eventlog.Event]:
    """Confirm orders whose stock has been reserved, in one transaction (see eventlog.consume)."""
    return eventlog.consume(conn, CONSUMER, [eventlog.STOCK_RESERVED], _confirm, batch_size)


def _confirm(conn: Conn, event: eventlog.Event) -> list[eventlog.NewEvent]:
    conn.execute(
        "UPDATE orders.orders SET status = 'confirmed'"
        " WHERE order_ref = %s AND status = 'accepted'",
        (event.key,),
    )
    return []
