from typing import Annotated

from fastapi import Depends, Request
from psycopg import Connection
from psycopg.rows import DictRow, dict_row
from psycopg_pool import ConnectionPool

Conn = Connection[DictRow]


def create_pool(database_url: str, *, max_size: int = 10) -> ConnectionPool[Conn]:
    """A connection pool that opens when entered: `with create_pool(url) as pool: ...`"""
    return ConnectionPool(
        database_url,
        connection_class=Conn,
        kwargs={"row_factory": dict_row},
        min_size=1,
        max_size=max_size,
        timeout=5,
        # Hand out only live connections, so a database restart doesn't surface as errors.
        check=ConnectionPool.check_connection,
        open=False,
    )


def get_pool(request: Request) -> ConnectionPool[Conn]:
    pool: ConnectionPool[Conn] = request.app.state.pool
    return pool


Pool = Annotated[ConnectionPool[Conn], Depends(get_pool)]
