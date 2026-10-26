"""Unhappy path 2: the worker goes down and orders are still accepted. Once it's back, stock
catches up and the orders are confirmed.

Runs its own worker, so stop any other one first. The API must be running (make api).

    uv run python scripts/outage.py
"""

import signal
import subprocess
import sys
import time
from collections import Counter
from typing import Any

import httpx2 as httpx

API = "http://localhost:8000"
WATCH = ["BAN-001", "MLK-002"]


def main() -> None:
    run_id = time.strftime("%H%M%S")
    orders: list[dict[str, Any]] = [
        {
            "order_ref": f"app-{run_id}-{n}",
            "customer_id": f"cust-{n}",
            "items": [{"sku": "BAN-001", "qty": 2}, {"sku": "MLK-002", "qty": 1}],
        }
        for n in range(1, 6)
    ]
    refs = [order["order_ref"] for order in orders]

    with httpx.Client(base_url=API, timeout=10) as api:
        step("1. Worker running and caught up")
        worker = start_worker()
        before = wait_until_caught_up(api, refs=[])
        show_stock(api)

        step("2. Outage: the worker stops")
        stop(worker)

        step("3. Orders are still accepted")
        for order in orders:
            response = api.post("/orders", json=order)
            print(f"   POST /orders {order['order_ref']} -> {response.status_code}")

        step("4. Stock hasn't moved, and the orders wait as 'accepted'")
        if show_stock(api) == 0:
            sys.exit("Nothing is pending, so another worker must be running. Stop it and retry.")
        show_statuses(api, refs)

        step("5. Recovery: the worker starts again and catches up")
        worker = start_worker()
        after = wait_until_caught_up(api, refs)
        stop(worker)

        step("6. Stock reduced once per order, and every order confirmed")
        show_stock(api)
        show_statuses(api, refs)
        for sku in WATCH:
            expected = sum(
                item["qty"] for order in orders for item in order["items"] if item["sku"] == sku
            )
            mark = "ok" if before[sku] - after[sku] == expected else "MISMATCH"
            print(f"   {sku}: {before[sku]} -> {after[sku]} (expected -{expected})  {mark}")


def step(title: str) -> None:
    print(f"\n== {title}", flush=True)


def show_stock(api: httpx.Client) -> int:
    """Print the watched skus' stock; returns the number of pending events."""
    stock = api.get("/stock").json()
    levels = {product["sku"]: product["stock"] for product in stock["products"]}
    print(
        "   "
        + "   ".join(f"{sku} stock={levels[sku]}" for sku in WATCH)
        + f"   pending_events={stock['pending_events']}"
    )
    return int(stock["pending_events"])


def show_statuses(api: httpx.Client, refs: list[str]) -> None:
    counts = Counter(order_status(api, ref) for ref in refs)
    print("   order statuses: " + ", ".join(f"{n} {status}" for status, n in counts.items()))


def order_status(api: httpx.Client, ref: str) -> str:
    return str(api.get(f"/orders/{ref}").json()["status"])


def start_worker() -> subprocess.Popen[Any]:
    worker = subprocess.Popen([sys.executable, "-m", "orderflow.worker"])
    time.sleep(1)  # let it start up, so its log lines show in order
    return worker


def stop(worker: subprocess.Popen[Any]) -> None:
    worker.send_signal(signal.SIGTERM)
    worker.wait(timeout=10)


def wait_until_caught_up(api: httpx.Client, refs: list[str], timeout: float = 15) -> dict[str, int]:
    """Stock levels, once stock reflects every accepted order and the given orders are confirmed."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        stock = api.get("/stock").json()
        confirmed = all(order_status(api, ref) == "confirmed" for ref in refs)
        if stock["pending_events"] == 0 and confirmed:
            return {product["sku"]: product["stock"] for product in stock["products"]}
        time.sleep(0.2)
    sys.exit("The worker didn't catch up in time.")


if __name__ == "__main__":
    main()
