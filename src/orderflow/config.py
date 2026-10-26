import os
from dataclasses import dataclass
from typing import Self

DEFAULT_DATABASE_URL = "postgresql://orderflow:orderflow@localhost:55432/orderflow"


@dataclass(frozen=True)
class Settings:
    database_url: str
    worker_batch_size: int
    worker_poll_seconds: float

    @classmethod
    def from_env(cls) -> Self:
        return cls(
            database_url=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL),
            worker_batch_size=int(os.environ.get("WORKER_BATCH_SIZE", "100")),
            worker_poll_seconds=float(os.environ.get("WORKER_POLL_SECONDS", "0.5")),
        )
