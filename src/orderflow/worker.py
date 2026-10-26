"""Background worker: runs the event log consumers.

    python -m orderflow.worker            # all consumers
    python -m orderflow.worker stock      # only some; each can also run as its own process

  stock   takes accepted orders off stock, then publishes stock.reserved
  orders  marks an order confirmed when its stock.reserved arrives

Stop it at any point (Ctrl+C or SIGTERM) and start it again later. Each consumer resumes from
its offset in Postgres, so an outage only delays the work: nothing is lost or done twice.
"""

import logging
import signal
import sys
import threading
from collections.abc import Callable

import psycopg
from psycopg_pool import ConnectionPool

from orderflow.config import Settings
from orderflow.db import Conn, create_pool
from orderflow.eventlog import Event
from orderflow.orders import service as orders
from orderflow.stock import service as stock

log = logging.getLogger("orderflow.worker")

Consumer = Callable[[Conn, int], list[Event]]

CONSUMERS: dict[str, Consumer] = {
    "stock": stock.process_next_batch,
    "orders": orders.process_next_batch,
}
MAX_BACKOFF_SECONDS = 10.0


def run(
    pool: ConnectionPool[Conn],
    consumers: dict[str, Consumer],
    batch_size: int,
    poll_seconds: float,
    stop: threading.Event,
) -> None:
    backoff = poll_seconds
    catching_up = False
    while not stop.is_set():
        try:
            batches = {
                name: _next_batch(pool, consume, batch_size) for name, consume in consumers.items()
            }
        except psycopg.OperationalError as exc:  # includes PoolTimeout: database unreachable
            log.warning("database unavailable (%s), retrying in %.1fs", exc, backoff)
            stop.wait(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            continue
        backoff = poll_seconds
        for name, events in batches.items():
            if events:
                log.info("%s: processed events %d-%d", name, events[0].id, events[-1].id)
        if any(batches.values()):
            catching_up = True
        else:
            if catching_up:
                log.info("up to date")
                catching_up = False
            stop.wait(poll_seconds)


def _next_batch(pool: ConnectionPool[Conn], consume: Consumer, batch_size: int) -> list[Event]:
    with pool.connection() as conn:
        return consume(conn, batch_size)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [worker] %(message)s")
    names = sys.argv[1:] or list(CONSUMERS)
    if unknown := set(names) - CONSUMERS.keys():
        sys.exit(
            f"Unknown consumer {', '.join(sorted(unknown))}. Choose from {', '.join(CONSUMERS)}."
        )
    settings = Settings.from_env()
    stop = threading.Event()
    # Finish the batch in hand, then exit.
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    with create_pool(settings.database_url, max_size=1) as pool:
        log.info("started: %s", ", ".join(names))
        consumers = {name: CONSUMERS[name] for name in names}
        run(pool, consumers, settings.worker_batch_size, settings.worker_poll_seconds, stop)
    log.info("stopped")


if __name__ == "__main__":
    main()
