"""Queue reads against ``agent_runs``.

The queue is a table, not a data structure. Nothing here caches: every question
is asked of the database, because the orchestrator has to be able to die and come
back without noticing.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg

from orchestrator.enums import TERMINAL_STATUSES, RunStatus

#: Statuses where a sandbox is supposed to exist.
ACTIVE_STATUSES = (RunStatus.LEASED.value, RunStatus.RUNNING.value)

_COLUMNS = (
    "id, kind, subject, status, attempts, worker_id, lease_expires_at, not_before"
)


@dataclass(frozen=True, slots=True)
class Run:
    id: int
    kind: str
    subject: str
    status: RunStatus
    attempts: int
    worker_id: str | None
    lease_expires_at: datetime | None
    #: NOT NULL in the schema; optional here only so hand-built Runs in tests
    #: need not care. Rows read from the database always carry a value.
    not_before: datetime | None = None

    def lease_expired(self, now: datetime) -> bool:
        """A missing lease counts as expired: it means nobody is holding this."""
        return self.lease_expires_at is None or self.lease_expires_at <= now


def _to_run(row: dict) -> Run:
    return Run(
        id=row["id"],
        kind=row["kind"],
        subject=row["subject"],
        status=RunStatus(row["status"]),
        attempts=row["attempts"],
        worker_id=row["worker_id"],
        lease_expires_at=row["lease_expires_at"],
        not_before=row["not_before"],
    )


def db_now(conn: psycopg.Connection) -> datetime:
    """The database's transaction clock.

    Backoff windows are compared against ``now()`` in SQL by
    :func:`claim_next_queued`, so they have to be *computed* from that same
    clock. Using the host's clock instead would let a skew between droplet and
    managed Postgres silently stretch or shrink every window.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT now() AS now")
        return cur.fetchone()["now"]


def claim_next_queued(conn: psycopg.Connection) -> Run | None:
    """Lock the oldest QUEUED run and return it, or None if there is nothing.

    ``FOR UPDATE SKIP LOCKED`` means two orchestrators ticking at the same moment
    take different rows instead of fighting over one. The lock is held until the
    caller's transaction ends, so the caller must append ``run_leased`` inside
    that same transaction or the claim means nothing.

    A run inside its backoff window (``not_before`` in the future, set by
    ``loop.Orchestrator._abandon``) is invisible here, which is the entire
    mechanism of #20: it keeps its queue position but yields its turn to newer
    work until the window passes.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_COLUMNS} FROM agent_runs"
            " WHERE status = %s AND not_before <= now()"
            " ORDER BY created_at, id"
            " LIMIT 1"
            " FOR UPDATE SKIP LOCKED",
            (RunStatus.QUEUED.value,),
        )
        row = cur.fetchone()
    return _to_run(row) if row else None


def active_runs(conn: psycopg.Connection) -> list[Run]:
    """Every run that believes it has a sandbox, across all workers.

    Deliberately not filtered by ``worker_id``: reconcile has to be able to clean
    up after an orchestrator that died and never came back, and the concurrency
    cap has to count those runs too or a stale lease lets the cap be exceeded.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_COLUMNS} FROM agent_runs WHERE status = ANY(%s) ORDER BY id",
            (list(ACTIVE_STATUSES),),
        )
        return [_to_run(row) for row in cur.fetchall()]


def extend_lease(
    conn: psycopg.Connection, run_id: int, worker_id: str, lease_seconds: int
) -> datetime | None:
    """Push a run's lease out. Returns the new expiry, or None if it was not active.

    No event is appended; see ``loop.Orchestrator._heartbeat`` for why. The status
    guard makes this a no-op against a run that finished between the read and this
    write, so a heartbeat can never resurrect a terminal run.
    """
    expires = datetime.now(UTC) + timedelta(seconds=lease_seconds)
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE agent_runs"
            " SET lease_expires_at = %s, worker_id = %s, updated_at = now()"
            " WHERE id = %s AND status = ANY(%s)"
            " RETURNING lease_expires_at",
            (expires, worker_id, run_id, list(ACTIVE_STATUSES)),
        )
        row = cur.fetchone()
    return row["lease_expires_at"] if row else None


def get_run(conn: psycopg.Connection, run_id: int) -> Run:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLUMNS} FROM agent_runs WHERE id = %s", (run_id,))
        row = cur.fetchone()
    if row is None:
        raise LookupError(f"no such run: {run_id}")
    return _to_run(row)


def queued_count(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n FROM agent_runs WHERE status = %s",
            (RunStatus.QUEUED.value,),
        )
        return cur.fetchone()["n"]


def has_recent_run(
    conn: psycopg.Connection, kind: str, subject: str, cooldown: timedelta
) -> bool:
    """Whether this subject is already queued, running, or recently finished.

    Two questions in one, because a work source needs both and they have the same
    answer: do not enqueue this. A non-terminal run means it is in flight. A
    terminal one inside the cooldown means a fix may already be on its way and
    the errors have not had time to stop.

    The partial unique index on ``(kind, subject)`` is the backstop if this is
    ever wrong; this exists so the normal path does not rely on catching an
    integrity error.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM agent_runs"
            " WHERE kind = %s AND subject = %s"
            "   AND (status <> ALL(%s) OR updated_at > now() - %s::interval)"
            " LIMIT 1",
            (
                kind,
                subject,
                [s.value for s in TERMINAL_STATUSES],
                f"{int(cooldown.total_seconds())} seconds",
            ),
        )
        return cur.fetchone() is not None
