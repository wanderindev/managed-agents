"""Queue reads against ``agent_runs``, and the supported way to cancel (#59).

The queue is a table, not a data structure. Nothing here caches: every question
is asked of the database, because the orchestrator has to be able to die and come
back without noticing.

Cancelling stale queued work is an operator command, not a hand-written row
(``agent_events`` is append-only, and a status edit would break the replay):

    python -m orchestrator.queue cancel --kind memory_dream --older-than 1d --dry-run
    python -m orchestrator.queue cancel --kind memory_dream --older-than 1d

A cancel is a ``run_abandoned`` event with ``requeued: false`` and
``cancelled: true`` (so the status fold lands on ABANDONED with no new
vocabulary), plus an ``email_sent`` marker saying why no outcome email follows.
Only QUEUED runs are touched; a run already leased or running is left alone.
"""

import argparse
import logging
import re
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg

from orchestrator import log
from orchestrator.db import connect
from orchestrator.enums import TERMINAL_STATUSES, EventType, RunStatus

logger = logging.getLogger(__name__)

#: Statuses where a sandbox is supposed to exist.
ACTIVE_STATUSES = (RunStatus.LEASED.value, RunStatus.RUNNING.value)

#: The scalar subquery pulls the opening ``run_queued`` payload along with the
#: row. That payload is the job's input (#6 writes it, #7 reads it), and having
#: it on the Run is what lets a spec builder work from the Run alone — the
#: runner layer stays free of database access.
_COLUMNS = (
    "id, kind, subject, status, attempts, worker_id, lease_expires_at, not_before,"
    " (SELECT e.payload FROM agent_events e"
    "   WHERE e.run_id = agent_runs.id AND e.type = 'run_queued'"
    "   ORDER BY e.seq LIMIT 1) AS payload"
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
    #: The ``run_queued`` event's payload: what the work source knew when it
    #: enqueued this. None only for hand-built Runs in tests.
    payload: dict | None = None

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
        payload=row["payload"],
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


def open_run(
    conn: psycopg.Connection,
    subject: str,
    *,
    kind: str | None = None,
    exclude_kind: str | None = None,
) -> Run | None:
    """The oldest non-terminal run for a subject, if any.

    Same status predicate as the partial unique index
    ``agent_runs_open_subject_key``, so "open" here means exactly "holds the
    uniqueness slot". Two callers, two filters: the Sentry poll asks with
    ``exclude_kind`` to see whether a *chained* run (review, revision) still
    owns the subject — an AWAITING_HUMAN review means a human owes a decision,
    and re-triaging can never be the right move in that state (#51). The loop
    asks with ``kind`` to name the run that just refused a chained insert.
    """
    clauses = ["subject = %s", "status <> ALL(%s)"]
    params: list = [subject, [s.value for s in TERMINAL_STATUSES]]
    if kind is not None:
        clauses.append("kind = %s")
        params.append(kind)
    if exclude_kind is not None:
        clauses.append("kind <> %s")
        params.append(exclude_kind)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_COLUMNS} FROM agent_runs"
            f" WHERE {' AND '.join(clauses)}"
            " ORDER BY id LIMIT 1",
            params,
        )
        row = cur.fetchone()
    return _to_run(row) if row else None


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


# --- cancelling ---------------------------------------------------------------

#: ``run_abandoned`` payload key marking an operator or coalescing cancel, as
#: distinct from a run the loop gave up on after its attempts.
CANCELLED_KEY = "cancelled"

_DURATION = re.compile(r"^\s*(\d+)\s*([smhdw])\s*$")
_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}


def parse_age(text: str) -> timedelta:
    """``30m``, ``12h``, ``1d``, ``2w`` -> a timedelta. Anything else is an error."""
    match = _DURATION.match(text)
    if not match:
        raise ValueError(f"not a duration (want e.g. 30m, 12h, 1d, 2w): {text!r}")
    return timedelta(**{_UNITS[match.group(2)]: int(match.group(1))})


def _cancel(conn: psycopg.Connection, run_id: int, reason: str) -> None:
    """Cancel one QUEUED run. Caller owns the transaction and the row lock.

    The ``email_sent`` marker goes in with it: a cancelled run is a deliberate
    non-outcome, and without the marker the notifier would mail "run ABANDONED
    after 0 attempts" for every one of them.
    """
    log.append(
        conn,
        run_id,
        EventType.RUN_ABANDONED,
        {"requeued": False, CANCELLED_KEY: True, "reason": reason},
        worker_id=None,
        lease_expires_at=None,
    )
    log.append(
        conn, run_id, EventType.EMAIL_SENT, {"suppressed": f"cancelled: {reason}"}
    )


def cancel_queued(
    conn: psycopg.Connection,
    *,
    reason: str,
    kind: str | None = None,
    older_than: timedelta | None = None,
    run_ids: Iterable[int] | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Cancel the QUEUED runs matching every given filter; return what matched.

    At least one filter is required, so a bare call can never empty the queue.
    ``older_than`` is measured from ``created_at`` on the database clock. Rows
    are locked ``SKIP LOCKED``, the same way the loop claims, so a run the loop
    is leasing at this instant is skipped rather than cancelled under it. A dry
    run takes the same path and writes nothing.
    """
    run_ids = list(run_ids or [])
    if kind is None and older_than is None and not run_ids:
        raise ValueError("refusing to cancel without a filter (kind, age or run id)")
    clauses = ["status = %s"]
    params: list = [RunStatus.QUEUED.value]
    if kind is not None:
        clauses.append("kind = %s")
        params.append(kind)
    if older_than is not None:
        clauses.append("created_at < now() - %s::interval")
        params.append(f"{int(older_than.total_seconds())} seconds")
    if run_ids:
        clauses.append("id = ANY(%s)")
        params.append(run_ids)
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, kind, subject, created_at FROM agent_runs"
                f" WHERE {' AND '.join(clauses)}"
                " ORDER BY created_at, id"
                " FOR UPDATE SKIP LOCKED",
                params,
            )
            rows = cur.fetchall()
        if not dry_run:
            for row in rows:
                _cancel(conn, row["id"], reason)
    return rows


def coalesce(conn: psycopg.Connection, kind: str, key: str) -> list[int]:
    """Keep only the newest QUEUED run of ``kind`` per ``payload[key]``.

    The rest are cancelled with a reason naming the run that superseded them.
    Built for dreaming (#59): each audit re-reads the same history, so after a
    pause eleven queued dreams for one repo are eleven copies of the newest
    one. Runs whose payload lacks ``key`` are left alone. Returns the ids
    cancelled.
    """
    cancelled: list[int] = []
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {_COLUMNS} FROM agent_runs"
                " WHERE kind = %s AND status = %s"
                " ORDER BY created_at DESC, id DESC"
                " FOR UPDATE SKIP LOCKED",
                (kind, RunStatus.QUEUED.value),
            )
            runs = [_to_run(row) for row in cur.fetchall()]
        newest: dict[str, Run] = {}
        for run in runs:
            group = (run.payload or {}).get(key)
            if group is None:
                continue
            keeper = newest.setdefault(group, run)
            if keeper is run:
                continue
            _cancel(
                conn,
                run.id,
                f"superseded by newer queued run {keeper.id} ({keeper.subject})",
            )
            cancelled.append(run.id)
    if cancelled:
        logger.info(
            "coalesced %s: cancelled %s superseded queued run(s) %s",
            kind,
            len(cancelled),
            cancelled,
        )
    return cancelled


def queued_by_kind(conn: psycopg.Connection) -> dict[str, int]:
    """QUEUED run counts per kind, for the paused-login reminder."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT kind, count(*) AS n FROM agent_runs WHERE status = %s"
            " GROUP BY kind ORDER BY kind",
            (RunStatus.QUEUED.value,),
        )
        return {row["kind"]: row["n"] for row in cur.fetchall()}


# --- the command --------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m orchestrator.queue",
        description="Operate on the run queue. Always --dry-run first.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    cancel = sub.add_parser(
        "cancel",
        help="cancel QUEUED runs (never leased or running ones)",
        description=(
            "Cancel QUEUED runs matching every filter given. At least one filter"
            " is required. Writes run_abandoned (cancelled) + email_sent events."
        ),
    )
    cancel.add_argument("--kind", help="only this run kind, e.g. memory_dream")
    cancel.add_argument(
        "--older-than",
        type=parse_age,
        metavar="AGE",
        help="only runs queued longer ago than this: 30m, 12h, 1d, 2w",
    )
    cancel.add_argument(
        "--run",
        type=int,
        action="append",
        dest="runs",
        metavar="ID",
        help="only this run id (repeatable)",
    )
    cancel.add_argument(
        "--reason",
        default="cancelled by the operator (python -m orchestrator.queue cancel)",
        help="recorded on each cancelled run",
    )
    cancel.add_argument(
        "--dry-run", action="store_true", help="list what would be cancelled"
    )
    args = parser.parse_args(argv)
    if args.kind is None and args.older_than is None and not args.runs:
        parser.error("cancel needs at least one of --kind, --older-than, --run")
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s %(message)s"
    )

    with connect() as conn:
        rows = cancel_queued(
            conn,
            reason=args.reason,
            kind=args.kind,
            older_than=args.older_than,
            run_ids=args.runs,
            dry_run=args.dry_run,
        )
    for row in rows:
        print(
            f"run {row['id']}  {row['kind']}  {row['subject']}"
            f"  queued {row['created_at'].isoformat(timespec='seconds')}"
        )
    verb = "would cancel" if args.dry_run else "cancelled"
    print(f"{verb} {len(rows)} queued run(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
