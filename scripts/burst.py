"""Unhappy path 1: duplicate submissions.

Fires a burst of orders at the API, 8 at a time, sending each order 1-3 times like a client
retrying after a timeout. Then it checks that stock went down by exactly one copy of each order.

    uv run python scripts/burst.py [--orders 10] [--prefix web-1234]

Pass the prefix of an earlier run to resubmit that whole burst: nothing new gets counted.
"""

import argparse
import random
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx2 as httpx

SKUS = ["BAN-001", "APL-003", "MLK-002", "BRD-004", "EGG-005"]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--orders", type=int, default=10)
    parser.add_argument("--prefix", default=f"web-{time.strftime('%H%M%S')}")
    args = parser.parse_args()

    rng = random.Random(args.prefix)  # the same prefix always produces the same orders
    orders = [make_order(f"{args.prefix}-{n:02d}", rng) for n in range(1, args.orders + 1)]
    requests = [order for order in orders for _ in range(rng.randint(1, 3))]
    rng.shuffle(requests)

    with httpx.Client(base_url=args.api, timeout=10) as api:
        before = settled_stock(api)
        print(f"Sending {len(requests)} requests for {len(orders)} orders, 8 at a time...\n")
        with ThreadPoolExecutor(8) as pool:
            statuses = list(
                pool.map(lambda order: api.post("/orders", json=order).status_code, requests)
            )

        by_ref: defaultdict[str, list[int]] = defaultdict(list)
        for order, status in zip(requests, statuses, strict=True):
            by_ref[order["order_ref"]].append(status)
        for ref, codes in sorted(by_ref.items()):
            print(f"  {ref}   {' '.join(map(str, sorted(codes, reverse=True)))}")
        counts = Counter(statuses)
        print(
            f"\n{counts[201]} created (201), {counts[200]} resubmissions answered with the"
            " stored order (200)\n"
        )

        created = [order for order in orders if 201 in by_ref[order["order_ref"]]]
        check_stock(api, before, created)


def make_order(order_ref: str, rng: random.Random) -> dict[str, Any]:
    skus = rng.sample(SKUS, rng.randint(1, 3))
    return {
        "order_ref": order_ref,
        "customer_id": f"cust-{rng.randint(1, 99)}",
        "items": [{"sku": sku, "qty": rng.randint(1, 3)} for sku in skus],
    }


def check_stock(
    api: httpx.Client, before: dict[str, int] | None, created: list[dict[str, Any]]
) -> None:
    after = settled_stock(api)
    if before is None or after is None:
        print("The stock worker isn't running or hasn't caught up, so stock wasn't checked.")
        print("Start it with `make worker`, then look at GET /stock.")
        return
    expected: Counter[str] = Counter()
    for order in created:
        for item in order["items"]:
            expected[item["sku"]] += item["qty"]
    print("sku        before  after  taken  expected")
    for sku in SKUS:
        taken = before[sku] - after[sku]
        mark = "ok" if taken == expected[sku] else "MISMATCH"
        print(f"{sku}    {before[sku]:>6} {after[sku]:>6} {taken:>6} {expected[sku]:>9}  {mark}")


def settled_stock(api: httpx.Client, timeout: float = 5) -> dict[str, int] | None:
    """Stock levels once the worker has applied every event, or None if it doesn't get there."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        stock = api.get("/stock").json()
        if stock["pending_events"] == 0:
            return {product["sku"]: product["stock"] for product in stock["products"]}
        time.sleep(0.2)
    return None


if __name__ == "__main__":
    main()
