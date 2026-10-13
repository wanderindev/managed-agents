"""Numbered plain-SQL migration runner.

Alembic is overkill for two tables and would drag a framework into a process
whose whole job is to be boring. Files are applied in filename order, each in its
own transaction, and recorded so a second run is a no-op.

    python -m orchestrator.migrate
"""

import sys
from pathlib import Path

import psycopg

from orchestrator.db import connect

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"

_ENSURE_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename   text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
)
"""


def pending(
    conn: psycopg.Connection, migrations_dir: Path = MIGRATIONS_DIR
) -> list[Path]:
    """Return the migration files not yet recorded as applied, in order."""
    with conn.cursor() as cur:
        cur.execute(_ENSURE_TABLE)
        cur.execute("SELECT filename FROM schema_migrations")
        applied = {row["filename"] for row in cur.fetchall()}
    conn.commit()
    return [p for p in sorted(migrations_dir.glob("*.sql")) if p.name not in applied]


def apply_all(
    conn: psycopg.Connection, migrations_dir: Path = MIGRATIONS_DIR
) -> list[str]:
    """Apply every pending migration. Returns the filenames applied."""
    applied: list[str] = []
    for path in pending(conn, migrations_dir):
        with conn.cursor() as cur:
            cur.execute(path.read_text())
            cur.execute(
                "INSERT INTO schema_migrations (filename) VALUES (%s)",
                (path.name,),
            )
        # One transaction per file: a failure half way through a run leaves the
        # earlier files applied and recorded, so a retry picks up where it stopped.
        conn.commit()
        applied.append(path.name)
    return applied


def main() -> int:
    with connect() as conn:
        applied = apply_all(conn)
    if applied:
        print("applied: " + ", ".join(applied))
    else:
        print("nothing to apply")
    return 0


if __name__ == "__main__":
    sys.exit(main())
