-- One schema per component. A component writes only to its own schema and to the event log,
-- and components talk through the log. The one direct call between them: orders asks stock
-- for prices (stock.get_prices) when it accepts an order.

CREATE SCHEMA stock;
CREATE SCHEMA orders;
CREATE SCHEMA eventlog;

-- Products and their stock levels, shaped like the brief's {sku, name, price_cents, stock}.
CREATE TABLE stock.products (
    sku         text PRIMARY KEY,
    name        text NOT NULL,
    price_cents integer NOT NULL CHECK (price_cents >= 0),
    stock       integer NOT NULL,             -- negative means oversold; orders are never refused for stock
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Every stock change, keyed by the event that caused it, so applying an event twice is a no-op.
CREATE TABLE stock.movements (
    event_id   bigint NOT NULL,
    sku        text NOT NULL REFERENCES stock.products,
    delta      integer NOT NULL,
    order_ref  text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, sku)
);

CREATE TABLE orders.orders (
    order_ref   text PRIMARY KEY,             -- client-supplied, and the idempotency key
    customer_id text NOT NULL,
    status      text NOT NULL CHECK (status IN ('accepted', 'confirmed')),
    total_cents bigint NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders.order_items (
    order_ref        text NOT NULL REFERENCES orders.orders,
    sku              text NOT NULL,
    qty              integer NOT NULL CHECK (qty > 0),
    unit_price_cents integer NOT NULL,        -- the price at the time of the order
    PRIMARY KEY (order_ref, sku)
);

-- Append-only log of domain events, in the role Kafka would play. A component appends events in
-- the same transaction as the change they describe (a transactional outbox), and consumers read
-- forward by id from their own offsets.
CREATE TABLE eventlog.events (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type       text NOT NULL,
    key        text NOT NULL,                 -- what the event is about, e.g. an order_ref
    data       jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX events_by_type ON eventlog.events (type, id);

-- How far each consumer has read, moved in the same transaction as the consumer's own writes.
CREATE TABLE eventlog.consumer_offsets (
    consumer      text PRIMARY KEY,
    last_event_id bigint NOT NULL DEFAULT 0,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

INSERT INTO eventlog.consumer_offsets (consumer) VALUES ('stock'), ('orders');
