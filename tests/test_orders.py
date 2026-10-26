import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from orderflow.orders import service
from orderflow.orders.models import OrderIn
from orderflow.stock import service as stock

ORDER = {
    "order_ref": "web-100045",
    "customer_id": "cust-42",
    "items": [{"sku": "BAN-001", "qty": 2}, {"sku": "APL-003", "qty": 1}],
}


def accepted_events(pool):
    with pool.connection() as conn:
        return conn.execute(
            "SELECT count(*) AS n FROM eventlog.events WHERE type = 'order.accepted'"
        ).fetchone()["n"]


def test_order_is_stored_and_priced_from_the_products(client):
    created = client.post("/orders", json=ORDER)

    assert created.status_code == 201
    assert created.json()["total_cents"] == 2 * 199 + 349
    assert created.json()["status"] == "accepted"
    assert client.get("/orders/web-100045").json() == created.json()


def test_total_keeps_the_price_at_the_time_of_the_order(client, pool):
    client.post("/orders", json=ORDER)
    with pool.connection() as conn:
        stock.upsert_product(conn, "BAN-001", "Bananas 1kg", 999, opening_stock=50)

    assert client.get("/orders/web-100045").json()["total_cents"] == 2 * 199 + 349


def test_order_is_confirmed_once_its_stock_is_reserved(client, run_worker):
    assert client.post("/orders", json=ORDER).json()["status"] == "accepted"

    run_worker()

    assert client.get("/orders/web-100045").json()["status"] == "confirmed"


def test_resubmitted_order_returns_the_original_and_counts_once(client, pool):
    first = client.post("/orders", json=ORDER)
    retry = client.post("/orders", json=ORDER)

    assert (first.status_code, retry.status_code) == (201, 200)
    assert retry.json() == first.json()
    assert accepted_events(pool) == 1


def test_concurrent_resubmissions_count_once(pool):
    order = OrderIn.model_validate(ORDER)
    start_together = threading.Barrier(10, timeout=5)

    def submit(_: int) -> bool:
        with pool.connection() as conn:
            start_together.wait()
            return service.create_order(conn, order)[1]

    with ThreadPoolExecutor(10) as executor:
        created = list(executor.map(submit, range(10)))

    assert created.count(True) == 1
    assert accepted_events(pool) == 1


def test_only_the_first_submission_of_an_order_ref_counts(client, pool):
    first = client.post("/orders", json=ORDER)
    changed = client.post("/orders", json={**ORDER, "items": [{"sku": "BAN-001", "qty": 5}]})

    assert changed.status_code == 200
    assert changed.json() == first.json()  # the stored order, not the new contents
    assert accepted_events(pool) == 1


def test_unknown_sku_is_rejected(client):
    response = client.post("/orders", json={**ORDER, "items": [{"sku": "NOPE-404", "qty": 1}]})

    assert response.status_code == 422


@pytest.mark.parametrize("qty", [0, -1, 1001])
def test_quantity_must_be_between_1_and_the_line_limit(client, qty):
    response = client.post("/orders", json={**ORDER, "items": [{"sku": "BAN-001", "qty": qty}]})

    assert response.status_code == 422


def test_order_ref_must_be_safe_to_use_in_a_url(client):
    response = client.post("/orders", json={**ORDER, "order_ref": "web/100045"})

    assert response.status_code == 422
