"""Apply the SQL files in migrations/ that haven't been applied yet: python -m orderflow.migrate"""

from pathlib import Path
from typing import Any

import psycopg

from orderflow.config import Settings

MIGRATIONS = Path(__file__).parent / "migrations"


def migrate(conn: psycopg.Connection[Any]) -> list[str]:
    """Apply pending migrations in one transaction and return their names."""
    with conn.transaction():
        conn.execute(
            "CREATE TABLE IF NOT EXISTS public.schema_migrations"
            " (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        done = {row[0] for row in conn.execute("SELECT version FROM public.schema_migrations")}
        pending = [path for path in sorted(MIGRATIONS.glob("*.sql")) if path.stem not in done]
        for path in pending:
            conn.execute(path.read_bytes())  # no parameters, so several statements per file is fine
            conn.execute("INSERT INTO public.schema_migrations (version) VALUES (%s)", (path.stem,))
    return [path.stem for path in pending]


def main() -> None:
    with psycopg.connect(Settings.from_env().database_url) as conn:
        applied = migrate(conn)
    print(f"Applied {', '.join(applied)}" if applied else "Database schema is up to date")


if __name__ == "__main__":
    main()
