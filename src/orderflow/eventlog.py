"""The append-only event log that connects the components.

It plays the part Kafka would: a component appends events in the same transaction as the change
they describe, and each consumer reads forward from its own offset. Moving to Kafka would
replace this module, not the components.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg.types.json import Jsonb

from orderflow.db import Conn

ORDER_ACCEPTED = "order.accepted"
STOCK_RESERVED = "stock.reserved"

# Any constant works, as long as every writer of the log uses the same one.
_APPEND_LOCK = 7_201_001


@dataclass(frozen=True)
class Event:
    id: int
    type: str
    key: str
    data: dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class NewEvent:
    type: str
    key: str
    data: dict[str, Any]


# Handles one event inside the consumer's transaction, and returns any events to publish.
Handler = Callable[[Conn, Event], list[NewEvent]]


def append(conn: Conn, event_type: str, key: str, data: dict[str, Any]) -> None:
    """Append an event as part of the caller's transaction.

    Ids come from a sequence, which hands them out when a row is inserted, not when it commits.
    Unchecked, event 11 could commit before event 10, and a reader that had already moved past
    11 would never see 10. This lock is held until the caller's transaction ends, so events
    commit in id order and readers can safely page with `id > after`. Since the lock serialises
    everything from here to commit, this should be the transaction's last write.
    """
    conn.execute("SELECT pg_advisory_xact_lock(%s)", (_APPEND_LOCK,))
    conn.execute(
        "INSERT INTO eventlog.events (type, key, data) VALUES (%s, %s, %s)",
        (event_type, key, Jsonb(data)),
    )


def read(conn: Conn, after: int, limit: int, types: Sequence[str]) -> list[Event]:
    """Up to `limit` events of the given types with ids greater than `after`, oldest first."""
    rows = conn.execute(
        "SELECT id, type, key, data, created_at FROM eventlog.events"
        " WHERE id > %s AND type = ANY(%s) ORDER BY id LIMIT %s",
        (after, list(types), limit),
    ).fetchall()
    return [Event(**row) for row in rows]


def consume(
    conn: Conn, consumer: str, types: Sequence[str], handle: Handler, batch_size: int
) -> list[Event]:
    """Pass the consumer's next batch of events to `handle`, and return the batch.

    The handler's writes, the events it publishes and the consumer's new offset commit in one
    transaction. A crash part-way through leaves nothing half-done, and the batch is redone
    from the stored offset next time.
    """
    with conn.transaction():
        # Locking the offset row makes a second copy of this consumer wait here, rather than
        # take the same batch.
        offset = conn.execute(
            "SELECT last_event_id FROM eventlog.consumer_offsets WHERE consumer = %s FOR UPDATE",
            (consumer,),
        ).fetchone()
        if offset is None:
            raise LookupError(f"no offset for consumer {consumer!r} in eventlog.consumer_offsets")
        events = read(conn, offset["last_event_id"], batch_size, types)
        published = [new for event in events for new in handle(conn, event)]
        if events:
            conn.execute(
                "UPDATE eventlog.consumer_offsets SET last_event_id = %s, updated_at = now()"
                " WHERE consumer = %s",
                (events[-1].id, consumer),
            )
        for new in published:  # last, as append() needs
            append(conn, new.type, new.key, new.data)
    return events


def position(conn: Conn, consumer: str, types: Sequence[str]) -> tuple[int, int]:
    """The consumer's offset, and how many events of the given types are waiting after it."""
    row = conn.execute(
        """
        SELECT last_event_id,
               (SELECT count(*) FROM eventlog.events
                WHERE id > last_event_id AND type = ANY(%s)) AS pending
        FROM eventlog.consumer_offsets WHERE consumer = %s
        """,
        (list(types), consumer),
    ).fetchone()
    if row is None:
        raise LookupError(f"no offset for consumer {consumer!r} in eventlog.consumer_offsets")
    return row["last_event_id"], row["pending"]
