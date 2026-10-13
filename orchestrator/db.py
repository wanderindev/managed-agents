"""Database access. psycopg3 directly, no ORM.

Two tables and a fold do not need a mapper, and the orchestrator has to stay
small enough to be obviously correct.
"""

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

from orchestrator import config


@contextmanager
def connect(dsn: str | None = None) -> Iterator[psycopg.Connection]:
    """Open a connection with dict rows and autocommit off.

    Callers own the transaction boundary. Everything in ``log.py`` assumes it is
    inside one, because appending an event and moving the run's status must be
    atomic or the log stops being trustworthy.
    """
    with psycopg.connect(dsn or config.DATABASE_URL, row_factory=dict_row) as conn:
        yield conn
