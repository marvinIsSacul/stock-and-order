# Solution: design and trade-offs

## The shape of it

```
client ──POST /orders──▶ api ──one transaction──▶ order (accepted) + items + order.accepted
                      (stateless)                                       │
                                                                        │ eventlog.events
                     ┌──────────────────────────────────────────────────┤ append-only, read by id
                     ▼                                                  ▼
       worker: stock consumer                                    GET /events?after=N
       one transaction: stock − qty, ledger rows,                (Option A: order.accepted
       its offset, and a stock.reserved event                     only; scripts/consume_feed.py)
                     │
                     ▼ stock.reserved
       worker: orders consumer
       one transaction: order → confirmed, its offset
```

Accepting an order never waits on stock. The order is stored as `accepted`, together with an `order.accepted` event, in one transaction. In the worker, the stock consumer takes each accepted order off stock and publishes `stock.reserved`. The orders consumer then marks the order `confirmed`. Other systems read accepted orders from the same log over HTTP. Everything that matters lives in Postgres, so any process can be stopped, restarted or duplicated.

## Components and boundaries

| Component | Owns | Uses from others | Runs in |
|---|---|---|---|
| orders | `orders.orders`, `orders.order_items` | prices, through `stock.get_prices()`; `stock.reserved` events | api (intake, reads); worker (status updates) |
| stock | `stock.products` (sku, name, price, stock), `stock.movements` | `order.accepted` events | worker (stock updates); api (reads) |
| event log | `eventlog.events`, `eventlog.consumer_offsets` | | shared plumbing, the role a Kafka cluster would play |

Each component writes only to its own schema and to the event log. The log is how the components talk: orders publishes `order.accepted`, and stock publishes `stock.reserved`. The one direct dependency is orders asking stock for prices through `stock.get_prices()`. That's a function in stock's public interface, not a read of stock's tables, so stock can change how it stores products without touching orders.

As the brief asks, the stock component manages products and their stock levels. A product is `{sku, name, price_cents, stock}`, as in the brief's example. Orders needs prices to accept an order, and must keep accepting orders while stock processing is down. That works because `get_prices` is a plain read in the API process and doesn't depend on the worker. If stock moved to its own database, orders would instead keep its own copy of prices, updated from product events.

There are two processes:

- **`api`** (FastAPI): order intake, order and stock reads, and the feed. It's stateless, so it can run as many copies as needed.
- **`worker`**: runs both consumers in one loop. The stock consumer takes accepted orders off stock and publishes `stock.reserved`, and the orders consumer confirms the orders. This is the part the brief describes as possibly unreachable. Because it's its own process, an outage is a real one: you stop the process. Restarting it also proves nothing depends on memory. Each consumer can also run as its own process (`python -m orderflow.worker stock`), which is how they'd be deployed in production.

## Unhappy path 1: duplicate submissions

A client whose request timed out can't tell whether the order was stored, so it retries. The design makes that retry safe.

- **`order_ref` is the primary key**, and intake does `INSERT … ON CONFLICT (order_ref) DO NOTHING RETURNING …`. If no row comes back, the order already exists.
- **Concurrent duplicates are handled by Postgres itself.** When two requests with the same `order_ref` race, the second insert waits on the unique index until the first transaction commits, and then inserts nothing. There is no check-then-insert race and no application lock. `test_concurrent_resubmissions_count_once` fires 10 identical submissions at once.
- **Only the first submission counts.** Every later submission of the same `order_ref` gets `200` with the stored order, whatever it contains. That's the brief's rule: "only one should count". A stricter API would compare contents and answer `409` when an `order_ref` is reused for a different order, which guards against client bugs. It goes beyond the brief, so I've left it as a noted option.
- **"Only one counts" holds downstream too.** The order, its items and its event are written in the same transaction. A duplicate inserts nothing, so it can't produce a second event, a second stock change or a second confirmation.
- **Prices are frozen at the time of the order.** Each item stores `unit_price_cents` and the order stores its total. Later price changes don't affect existing orders.
- **`order_ref` must be URL-safe**: letters, digits, `.`, `_`, `:` and `-`, starting with a letter or digit. It appears in `GET /orders/{order_ref}`, so an order like `web/1` would otherwise be stored but impossible to fetch.

I chose not to deduplicate in Redis or in memory. That would be a second source of truth outside the transaction, and in-memory state breaks as soon as there are two API instances. The unique constraint is atomic with the write.

## Order lifecycle: accepted, then confirmed

- An order is **`accepted`** once it's stored. That is the moment intake promises to the client.
- In the same transaction that takes the order's items off stock, the stock consumer publishes **`stock.reserved`**. The orders consumer turns that into **`confirmed`**. Its update only touches orders still in `accepted`, so a repeated event changes nothing.
- **Why an event rather than having stock update the order:** stock never writes to orders' tables. Each component owns its data, and the same pattern works unchanged across separate services or over Kafka.
- **During an outage, orders stay `accepted`** and become `confirmed` once the worker catches up. The order itself shows whether its stock has been handled.
- **There is no `short` status.** Insufficient stock is out of scope per the brief, so an oversold order is still confirmed and its product shows negative stock. The natural next step is for stock to publish `stock.short`, and for orders to move the order to `short`.

## Unhappy path 2: worker outage and catch-up

- **Intake never calls the worker.** If the worker is down, orders are still accepted, and their events wait in the log.
- **One batch is one transaction.** Both consumers share the same loop, `eventlog.consume`:
  1. Lock the consumer's offset row.
  2. Read up to 100 events of the types it subscribes to, after its offset.
  3. Handle each event.
  4. Move the offset to the last event read.
  5. Append any events the batch publishes (such as `stock.reserved`).
  6. Commit.

  If the worker is killed at any point, the transaction rolls back and the batch is redone on restart. Each stock change and each confirmation is applied exactly once.
- **The ledger makes reapplying harmless.** `stock.movements` has one row per (event, sku), and that pair is its primary key. Stock only changes when the ledger insert actually inserts a row, so applying an event twice does nothing and publishes nothing. Rewinding the offset, for example to rebuild, is safe, and every stock number can be traced back to the orders behind it.
- **A second worker can't double-apply.** The offset row is read with `FOR UPDATE`, so a second copy of a consumer started by mistake waits instead. This is a safety measure, not a way to scale: one consumer owns the log at a time. Scaling out would mean partitioning the log, the way Kafka partitions do.
- **Freshness is part of the API.** `GET /stock/{sku}` returns `applied_up_to_event` and `pending_events`, both read in one snapshot. During an outage `pending_events` climbs, and after catch-up it returns to 0. In production this is the consumer-lag metric you'd alert on.
- **Database blips are survivable.**
  - The API answers `503` with `Retry-After`. That's safe to retry because intake is idempotent.
  - The worker backs off from 0.5s up to 10s and then carries on.
  - The pool checks each connection before handing it out, so connections broken by a restart are discarded.

## The event log, and a subtle ordering problem

`eventlog.events` plays the part Kafka would. Components append events, and each consumer reads forward by id from its own saved offset, only picking up the event types it subscribes to.

Reading by id has a trap. Ids come from a sequence and are assigned when a row is **inserted, not when it commits**. Suppose order A gets event id 10 and order B gets 11, but B commits first. A reader asking for `id > 9` sees 11 and saves its position as 11. When A commits a moment later, event 10 is behind the reader and is never read. For the stock consumer that means stock that never catches up. For an external consumer it means a missing order.

The fix: `eventlog.append` takes a **transaction-scoped advisory lock** just before inserting the event, and every transaction appends as its last write. The lock is held until commit, so events become visible in id order and paging by id is safe. Consumers append the events they publish at the end of their batch for the same reason. `test_feed_never_skips_an_event_that_commits_late` holds one order uncommitted and checks that a second can't overtake it. The test fails if the lock is removed.

The cost is that transactions go through this final step (the appends plus the commit) one at a time. That puts a ceiling on throughput, which is fine at this scale. When it stops being fine, there are two options:

- Store each event's transaction id and only serve events from transactions older than the oldest one still running (`pg_snapshot_xmin(pg_current_snapshot())`). Writers no longer queue, but any long-running transaction delays the feed.
- Move the log to Kafka. Keep this table as an outbox, relay it with Debezium or a small publisher, and let partition offsets carry the ordering. `eventlog.py` is the only module that would change.

## Option A: the order feed

`GET /events?after=<id>&limit=<n>` serves `order.accepted` events from the same log. Internal events such as `stock.reserved` aren't exposed, because they're a contract between our own components, not for other teams.

- **Each consumer keeps its own position**, like a Kafka consumer offset. Consumers read at their own pace, can replay from 0, and an outage on either side only causes a delay.
- **Delivery is at-least-once.** A consumer saves its position after processing an event. If it crashes in between, it sees that event again, so it should ignore event ids it has already handled. `scripts/consume_feed.py` shows the pattern, including resuming after a restart.
- **The contract** is `type` plus `data`, where `data` is the order as accepted, the same shape `GET /orders/{ref}` returns. Changes to it are additive. A breaking change would get a new event type, such as `order.accepted.v2`, published alongside the old one until consumers have moved.
- **I chose a pull feed over webhooks.** We hold no delivery or retry state, consumers recover on their own, and a slow consumer can't back up intake. Webhooks could be added later as just another consumer of the log.
- **I chose Option A over Option B (the report)** because the log already existed, so the feed cost one endpoint. It also lets other teams build whatever view they need, including that daily report, without querying our tables.

## Trade-offs

| Decision | Why | Cost, or what I'd revisit |
|---|---|---|
| One Postgres, a schema per component | The outbox only works if the event is in the same transaction as the change; there's one thing to run | The shared database is a coupling point. At scale, each component gets its own database, with Kafka between them |
| Postgres as the event log, polled every 0.5s | No extra infrastructure; reading by id is simple and replayable | Up to about 0.5s of delay at each step (`LISTEN/NOTIFY` could wake consumers immediately); old events are never cleaned up |
| A repeat `order_ref` always gets the stored order | It's the brief's rule: only one should count | A client that reuses an `order_ref` for a genuinely different order silently gets the old one back. A stricter API would answer `409` |
| Input limits: `qty` 1–1000, up to 100 lines, ids up to 64 characters | The minimum stops negative quantities from adding stock. The maximums keep bad input out of the database: a huge `qty` would overflow the column and fail as a 500 | The maximums are placeholders. Real limits come from the business |
| Orders gets prices from stock directly (`stock.get_prices`) | Follows the brief (stock manages products); simple; works while the worker is down | It's a runtime dependency on stock's data. With separate databases, orders would keep a copy of prices updated from product events |
| No stock check at intake | It's what keeps intake available when stock processing isn't | Stock can go negative, and an oversold order is still `confirmed`. That's visible and traceable through the ledger, but resolving it is out of scope |
| Both consumers in one worker process | Two processes, as the brief suggests, and one thing to stop for the outage | Stopping it pauses confirmations too. In production each consumer would be its own deployment, which is already supported |
| Sync FastAPI handlers, psycopg 3, raw SQL | The interesting parts are SQL behaviour (`ON CONFLICT`, row and advisory locks), which an ORM would hide. Threadpool handlers are plenty for database-bound work | Async pays off with a lot of slow I/O fan-out, which this doesn't have |
| The worker stops on unexpected errors | A bad event shouldn't be skipped silently | There's no dead-letter handling: a poison event halts that consumer until fixed (alert on lag) |

## Deliberately out of scope

- Auth and rate limiting (the brief says no auth).
- Insufficient-stock handling, and order states beyond `confirmed`: short, cancelled, refunded.
- Dead-letter handling and event retention.
- Metrics and tracing. `pending_events` is the one signal exposed.
- Containers for the API and worker (optional per the brief). Postgres runs in Docker, and the two processes are one command each.

## Testing

The tests run against a real Postgres, because the guarantees being tested are database behaviour: waits on a unique index, row and advisory locks, and transactional rollback. `make test` creates a separate `orderflow_test` database.

- **Orders**
  - Totals come from current prices and are kept as they were at the time of the order.
  - An order is confirmed once its stock is reserved.
  - A resubmission returns the original order and counts once.
  - 10 concurrent resubmissions produce one order and one event.
  - Only the first submission of an `order_ref` counts, even when a later one has different items.
  - An unknown sku or a non-URL-safe `order_ref` gets `422`.
- **Stock**
  - The worker takes orders off stock.
  - Orders are accepted while the worker is down: they stay `accepted` while stock waits, then stock catches up and every order is `confirmed`.
  - A crash mid-batch rolls the whole batch back, and the retry applies it once.
  - Rewinding the offset doesn't apply anything twice.
  - Two workers running at once still apply each order once.
- **Feed**
  - Pages arrive in order, and contain only `order.accepted`.
  - An event that commits late is never skipped.
