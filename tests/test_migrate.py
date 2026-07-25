"""The migration runner."""

import psycopg
import pytest
from psycopg.rows import dict_row

from orchestrator.migrate import apply_all, pending


def _table_exists(conn, name):
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) AS oid", (name,))
        return cur.fetchone()["oid"] is not None


def test_migrations_created_both_tables(conn):
    assert _table_exists(conn, "agent_runs")
    assert _table_exists(conn, "agent_events")


def test_apply_all_is_idempotent(conn):
    """The session fixture already ran them, so a second pass must do nothing."""
    assert apply_all(conn) == []
    assert pending(conn) == []


def test_every_migration_is_recorded(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM schema_migrations")
        recorded = cur.fetchone()["n"]
    assert recorded >= 1


def test_a_failing_migration_leaves_nothing_behind(migrated_dsn, tmp_path):
    """A broken file must not record itself as applied."""
    (tmp_path / "900_broken.sql").write_text("CREATE TABLE not_valid_sql (;")
    with psycopg.connect(migrated_dsn, row_factory=dict_row) as conn:
        with pytest.raises(psycopg.errors.SyntaxError):
            apply_all(conn, tmp_path)
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) AS n FROM schema_migrations WHERE filename = %s",
                ("900_broken.sql",),
            )
            assert cur.fetchone()["n"] == 0
