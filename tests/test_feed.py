import threading

from orderflow import eventlog
from orderflow.orders import service
from orderflow.orders.models import OrderIn


def order(ref):
    return {"order_ref": ref, "customer_id": "cust-1", "items": [{"sku": "APL-003", "qty": 1}]}


def test_feed_pages_through_accepted_orders_in_order(client, run_worker):
    for ref in ("web-1", "web-2", "web-3"):
        client.post("/orders", json=order(ref))
    run_worker()  # adds stock.reserved events to the log, which the feed leaves out

    first = client.get("/events", params={"after": 0, "limit": 2}).json()
    rest = client.get("/events", params={"after": first["next_after"]}).json()
    caught_up = client.get("/events", params={"after": rest["next_after"]}).json()

    assert [event["key"] for event in first["events"]] == ["web-1", "web-2"]
    assert [event["key"] for event in rest["events"]] == ["web-3"]
    assert {event["type"] for event in first["events"] + rest["events"]} == {"order.accepted"}
    assert rest["events"][0]["data"]["total_cents"] == 349
    assert caught_up == {"events": [], "next_after": 3}


def test_feed_never_skips_an_event_that_commits_late(pool):
    def keys_in_feed():
        with pool.connection() as conn:
            events = eventlog.read(conn, after=0, limit=10, types=[eventlog.ORDER_ACCEPTED])
            return [event.key for event in events]

    def submit(ref):
        with pool.connection() as conn:
            service.create_order(conn, OrderIn.model_validate(order(ref)))

    with pool.connection() as slow, slow.transaction():
        service.create_order(slow, OrderIn.model_validate(order("first")))  # not committed yet
        second = threading.Thread(target=submit, args=("second",))
        second.start()
        second.join(timeout=0.5)

        # "second" can't take an event id until "first" commits, so a reader can never see
        # "second" first and move past the spot where "first" lands.
        assert second.is_alive()
        assert keys_in_feed() == []
    second.join()

    assert keys_in_feed() == ["first", "second"]
