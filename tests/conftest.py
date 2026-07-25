"""Test fixtures.

A real PostgreSQL container, same convention as feliu-dev's backend suite. The
whole point of this module is trigger and constraint behaviour, so there is
nothing here a fake database could tell us.
"""

import psycopg
import pytest
from psycopg.rows import dict_row
from testcontainers.community.postgres import PostgresContainer

from orchestrator.migrate import apply_all


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def migrated_dsn(postgres_container):
    """Apply every migration once for the session, return the DSN."""
    dsn = postgres_container.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql://"
    )
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        apply_all(conn)
    return dsn


@pytest.fixture()
def conn(migrated_dsn):
    """A connection whose work is rolled back after each test.

    Rollback rather than TRUNCATE, because ``agent_events`` refuses to be
    truncated and that refusal is a feature under test.
    """
    with psycopg.connect(migrated_dsn, row_factory=dict_row) as connection:
        yield connection
        connection.rollback()
