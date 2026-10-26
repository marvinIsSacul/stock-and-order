from concurrent.futures import ThreadPoolExecutor

import pytest

from orderflow.stock import service


def order(ref, qty=2):
    return {"order_ref": ref, "customer_id": "cust-1", "items": [{"sku": "BAN-001", "qty": qty}]}


def bananas(client):
    return client.get("/stock/BAN-001").json()


def statuses(client, refs):
    return {client.get(f"/orders/{ref}").json()["status"] for ref in refs}


def test_worker_takes_accepted_orders_off_stock(client, run_worker):
    client.post("/orders", json=order("web-1", qty=3))
    run_worker()

    assert (bananas(client)["stock"], bananas(client)["pending_events"]) == (47, 0)


def test_orders_are_accepted_while_the_worker_is_down_and_everything_catches_up(client, run_worker):
    # No worker running: orders are still accepted, and their stock changes wait in the log.
    refs = [f"web-{n}" for n in range(5)]
    for ref in refs:
        assert client.post("/orders", json=order(ref)).status_code == 201
    during = bananas(client)
    assert (during["stock"], during["pending_events"]) == (50, 5)
    assert statuses(client, refs) == {"accepted"}

    assert run_worker()["stock"] == 5  # the worker comes back

    after = bananas(client)
    assert (after["stock"], after["pending_events"], after["applied_up_to_event"]) == (40, 0, 5)
    assert statuses(client, refs) == {"confirmed"}


def test_a_crash_mid_batch_applies_nothing_and_the_retry_applies_everything_once(
    client, pool, run_worker, monkeypatch
):
    for n in range(3):
        client.post("/orders", json=order(f"web-{n}"))
    reserve_stock = service._reserve_stock
    seen = []

    def crash_on_second_event(conn, event):
        seen.append(event.id)
        if len(seen) == 2:
            raise RuntimeError("worker killed")
        return reserve_stock(conn, event)

    monkeypatch.setattr(service, "_reserve_stock", crash_on_second_event)
    with pool.connection() as conn, pytest.raises(RuntimeError):
        service.process_next_batch(conn, batch_size=10)
    assert bananas(client)["stock"] == 50  # the first event's change was rolled back too

    monkeypatch.undo()
    run_worker()
    assert bananas(client)["stock"] == 44


def test_replaying_events_does_not_take_stock_twice(client, pool, run_worker):
    client.post("/orders", json=order("web-1"))
    run_worker()
    with pool.connection() as conn:  # rewind the stock consumer to the start of the log
        conn.execute(
            "UPDATE eventlog.consumer_offsets SET last_event_id = 0 WHERE consumer = 'stock'"
        )

    assert run_worker()["stock"] == 1
    assert bananas(client)["stock"] == 48


def test_two_workers_running_at_once_apply_each_order_once(client, run_worker):
    for n in range(20):
        client.post("/orders", json=order(f"web-{n}", qty=1))

    with ThreadPoolExecutor(2) as executor:
        processed = list(executor.map(lambda _: run_worker(), range(2)))

    assert sum(counts["stock"] for counts in processed) == 20
    assert bananas(client)["stock"] == 30
