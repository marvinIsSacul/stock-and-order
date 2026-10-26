"""Example consumer of the order feed (Option A): prints accepted orders as they arrive.

It saves its position to a file, so if you stop it and start it again it carries on where it
left off, just like a Kafka consumer committing its offset.

    uv run python scripts/consume_feed.py [--from-start] [--once]
"""

import argparse
import time
from pathlib import Path
from typing import Any

import httpx2 as httpx


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--position-file", type=Path, default=Path(".feed-cursor"))
    parser.add_argument("--from-start", action="store_true", help="ignore the saved position")
    parser.add_argument(
        "--once", action="store_true", help="exit when caught up instead of waiting"
    )
    args = parser.parse_args()

    position: Path = args.position_file
    after = 0 if args.from_start or not position.exists() else int(position.read_text())
    print(f"Reading the order feed after event {after}")

    with httpx.Client(base_url=args.api, timeout=10) as api:
        try:
            while True:
                try:
                    response = api.get("/events", params={"after": after, "limit": 100})
                    response.raise_for_status()
                except httpx.HTTPError as exc:
                    print(f"Feed unavailable ({exc}); retrying in 2s")
                    time.sleep(2)
                    continue
                events = response.json()["events"]
                for event in events:
                    handle(event)
                    after = event["id"]
                    # Saved after handling: a crash in between replays this event on restart
                    # (at-least-once), so a real handler should ignore event ids it has seen.
                    position.write_text(str(after))
                if not events:
                    if args.once:
                        break
                    time.sleep(1)
        except KeyboardInterrupt:
            pass
    print(f"Stopped after event {after}")


def handle(event: dict[str, Any]) -> None:
    order = event["data"]
    items = ", ".join(f"{item['qty']}x {item['sku']}" for item in order["items"])
    print(
        f"#{event['id']:<4} {event['type']}  {event['key']:<16} {order['customer_id']:<8}"
        f" R{order['total_cents'] / 100:>8.2f}  {items}"
    )


if __name__ == "__main__":
    main()
