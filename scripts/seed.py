"""Seed a few products with opening stock: uv run python scripts/seed.py

Safe to re-run: names and prices are updated, existing stock levels are left alone.
"""

import psycopg
from psycopg.rows import dict_row

from orderflow.config import Settings
from orderflow.stock import service as stock

PRODUCTS = [
    # sku, name, price in cents, opening stock
    ("BAN-001", "Bananas 1kg", 199, 50),
    ("APL-003", "Apples 1.5kg", 349, 40),
    ("MLK-002", "Full cream milk 2L", 2899, 30),
    ("BRD-004", "Brown bread 700g", 1699, 25),
    ("EGG-005", "Large eggs 6s", 3299, 20),
]


def main() -> None:
    with psycopg.connect(Settings.from_env().database_url, row_factory=dict_row) as conn:
        for sku, name, price_cents, opening_stock in PRODUCTS:
            stock.upsert_product(conn, sku, name, price_cents, opening_stock)
    print(f"Seeded {len(PRODUCTS)} products: {', '.join(sku for sku, *_ in PRODUCTS)}")


if __name__ == "__main__":
    main()
