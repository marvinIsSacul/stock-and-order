# Orders & Stock

A small, reliable slice of an order flow:

- an **Orders API** that accepts orders and counts each `order_ref` once, however many times it's submitted;
- **stock handling** that takes accepted orders off stock in a background worker and catches up after an outage. Each order then moves from `accepted` to `confirmed`;
- an **order feed** that other systems can consume (Option A).

Python 3.13, FastAPI, PostgreSQL. The design and trade-offs are in [SOLUTION.md](SOLUTION.md).

## Requirements

- Docker, to run Postgres
- [uv](https://docs.astral.sh/uv/getting-started/installation/), which installs Python 3.13 and the dependencies on first run
- `make`

## Run it

```sh
make setup    # start Postgres on localhost:55432, create the schema, seed 5 products
make api      # terminal 1: the API on http://localhost:8000 (interactive docs at /docs)
make worker   # terminal 2: the worker (takes orders off stock, then confirms them)
```

Place an order and look at the result:

```sh
curl -X POST localhost:8000/orders -H 'content-type: application/json' -d '{
  "order_ref": "web-100045", "customer_id": "cust-42",
  "items": [{"sku": "BAN-001", "qty": 2}, {"sku": "APL-003", "qty": 1}]}'

curl localhost:8000/orders/web-100045   # "status": "confirmed" once the worker has reserved its stock
curl localhost:8000/stock/BAN-001
```

## The two unhappy paths

**1. Duplicate submissions**

```sh
make burst
```

This seeds the products if needed, then sends about 20 requests for 10 orders, 8 at a time, with each order sent 1–3 times. You should see one `201` per order, `200` for every resubmission (each returns the stored order), and stock reduced by exactly one copy of each order. Run `uv run python scripts/burst.py --prefix <prefix from the first run>` to send a whole burst again: nothing is counted twice.

**2. Worker outage and catch-up**

Stop `make worker` first (Ctrl+C), then run:

```sh
make outage
```

The script runs its own worker. It stops the worker and places 5 orders. They're all accepted (`201`) and stay `accepted`, while stock doesn't move and shows `pending_events=5`. When the script starts the worker again, stock catches up with each order applied once, and all 5 orders become `confirmed`.

To do it by hand instead: stop the worker and place some orders. `pending_events` grows in `curl localhost:8000/stock/BAN-001` while the orders stay `accepted`. Start `make worker` again and everything catches up.

## Order feed (Option A)

```sh
make consume
```

This prints accepted orders as they arrive and saves its position in `.feed-cursor`. Stop it and start it again, and it carries on where it left off. Add `--from-start` to replay everything.

## API

| Method | Path | |
|---|---|---|
| `POST` | `/orders` | Accept an order. `201` created; `200` the `order_ref` was accepted before (returns the stored order); `422` invalid order (for example, an `order_ref` that isn't URL-safe) or unknown sku |
| `GET` | `/orders/{order_ref}` | Order details and status: `accepted`, then `confirmed` once its stock is reserved |
| `GET` | `/stock/{sku}` | The product and its current stock (`sku`, `name`, `price_cents`, `stock`), plus `pending_events`: accepted orders not reflected yet |
| `GET` | `/stock` | All products and their stock |
| `GET` | `/events?after=0&limit=100` | The order feed: `order.accepted` events after the id you last processed |
| `GET` | `/healthz` | Liveness, including the database |

## Tests and checks

```sh
make test     # needs `make db`; uses a separate orderflow_test database
make check    # ruff, and mypy --strict
```

## Layout

```
src/orderflow/
  api.py          HTTP app (stateless)
  worker.py       background worker: runs the stock and orders consumers
  eventlog.py     the append-only event log, and the consumer loop both components use
  orders/         order intake and status, and the order feed (GET /events): models, service, routes, feed
  stock/          products, prices and stock levels: models, service, routes
  migrations/     SQL schema
scripts/          seed, burst, outage and consume_feed demos
tests/
```

## Configuration

| Variable | Default |
|---|---|
| `DATABASE_URL` | `postgresql://orderflow:orderflow@localhost:55432/orderflow` |
| `TEST_DATABASE_URL` | `postgresql://orderflow:orderflow@localhost:55432/orderflow_test` |
| `WORKER_BATCH_SIZE` | `100` |
| `WORKER_POLL_SECONDS` | `0.5` |

Each consumer can also run as its own process: `uv run python -m orderflow.worker stock` and `uv run python -m orderflow.worker orders`.

`make reset` deletes all data and sets everything up again.
