"""The append-only log, and the fold that derives a run's status from it.

The invariant this module exists to protect: replaying a run's events reproduces
``agent_runs.status`` exactly. It holds *by construction* rather than by
discipline, because :func:`append` computes the stored status with the same
:func:`apply_event` that :func:`replay_status` uses. There is no second code path
that can drift.

Everything here assumes it is running inside a caller-owned transaction.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from orchestrator import config
from orchestrator.enums import EventType, RunStatus

#: Columns :func:`append` will write on ``agent_runs`` alongside the status.
#: A whitelist, because the keys are interpolated into SQL.
_RUN_FIELDS = frozenset({"worker_id", "lease_expires_at", "attempts"})


@dataclass(frozen=True, slots=True)
class Event:
    seq: int
    type: EventType
    payload: dict[str, Any]
    created_at: datetime


def truncate_payload(
    payload: dict[str, Any], max_bytes: int | None = None
) -> dict[str, Any]:
    """Cap one payload's serialized size.

    Applied inside :func:`append` rather than at the call sites, so no caller can
    forget. A single runaway tool result must not be able to bloat a log that is
    meant to be read, and nothing downstream gets value from a 2MB payload.

    The replacement keeps the fields you actually navigate by (``type``, ``name``)
    plus a preview, so a truncated event is still identifiable rather than a hole.
    """
    limit = config.MAX_PAYLOAD_BYTES if max_bytes is None else max_bytes
    encoded = json.dumps(payload, default=str)
    if len(encoded) <= limit:
        return payload
    return {
        "_truncated": True,
        "_original_bytes": len(encoded),
        "type": payload.get("type"),
        "name": payload.get("name"),
        "preview": encoded[: max(limit // 4, 0)],
    }


def apply_event(
    status: RunStatus | None,
    event_type: EventType,
    payload: dict[str, Any] | None = None,
) -> RunStatus | None:
    """Fold one event onto a status.

    Events that carry no lifecycle meaning (``claude_event``, ``stage_completed``,
    ``artifact``) leave the status alone. That is deliberate: a run's narrative is
    much longer than its state machine, and conflating the two is what makes
    resume (#12) impossible.
    """
    payload = payload or {}

    match event_type:
        case EventType.RUN_QUEUED:
            return RunStatus.QUEUED
        case EventType.RUN_LEASED:
            return RunStatus.LEASED
        case EventType.SANDBOX_STARTED:
            return RunStatus.RUNNING
        case EventType.HUMAN_GATE:
            return RunStatus.AWAITING_HUMAN
        case EventType.RUN_ABANDONED:
            # A dead lease normally goes back in the queue. Past the attempts
            # ceiling the reconcile pass (#4) sets requeued=False and the run
            # stops here, which also frees its (kind, subject) slot.
            #
            # A *missing* key therefore means terminal, not requeued. That is the
            # safe default (it cannot cause a retry loop) but it is a silent one,
            # so #4 must always write the flag explicitly.
            return RunStatus.QUEUED if payload.get("requeued") else RunStatus.ABANDONED
        case EventType.RUN_FAILED:
            return RunStatus.FAILED
        case EventType.RUN_DONE:
            return RunStatus.DONE
        case _:
            return status


def replay_status(events: list[Event]) -> RunStatus | None:
    """Derive a run's status from its history alone.

    This is the acceptance criterion for issue #3, so it reads only the events
    and never ``agent_runs.status``.
    """
    status: RunStatus | None = None
    for event in events:
        status = apply_event(status, event.type, event.payload)
    return status


def create_run(
    conn: psycopg.Connection,
    kind: str,
    subject: str,
    payload: dict[str, Any] | None = None,
) -> int:
    """Insert a QUEUED run and its opening ``run_queued`` event.

    Raises ``psycopg.errors.UniqueViolation`` if a non-terminal run already
    exists for this ``(kind, subject)``. Callers treat that as "already enqueued",
    not as an error; see #6.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO agent_runs (kind, subject, status) VALUES (%s, %s, %s)"
            " RETURNING id",
            (kind, subject, RunStatus.QUEUED.value),
        )
        run_id = cur.fetchone()["id"]
        cur.execute(
            "INSERT INTO agent_events (run_id, seq, type, payload)"
            " VALUES (%s, 1, %s, %s)",
            (run_id, EventType.RUN_QUEUED.value, Jsonb(payload or {})),
        )
    return run_id


def append(
    conn: psycopg.Connection,
    run_id: int,
    event_type: EventType,
    payload: dict[str, Any] | None = None,
    **run_fields: Any,
) -> int:
    """Append an event and move the run's status to match. Returns the new seq.

    ``run_fields`` sets whitelisted ``agent_runs`` columns in the same statement,
    so a lease (``run_leased`` plus ``worker_id`` and ``lease_expires_at``) lands
    atomically instead of as two writes that can be interrupted between.
    """
    unknown = set(run_fields) - _RUN_FIELDS
    if unknown:
        raise ValueError(f"not a writable agent_runs column: {sorted(unknown)}")

    with conn.cursor() as cur:
        # FOR UPDATE serialises appends for this run, which both gives us the
        # current status and removes the race on the next seq. Per-run locking,
        # so unrelated runs never wait on each other.
        cur.execute("SELECT status FROM agent_runs WHERE id = %s FOR UPDATE", (run_id,))
        row = cur.fetchone()
        if row is None:
            raise LookupError(f"no such run: {run_id}")

        new_status = apply_event(RunStatus(row["status"]), event_type, payload)

        cur.execute(
            "INSERT INTO agent_events (run_id, seq, type, payload)"
            " SELECT %s, coalesce(max(seq), 0) + 1, %s, %s"
            " FROM agent_events WHERE run_id = %s"
            " RETURNING seq",
            (
                run_id,
                event_type.value,
                Jsonb(truncate_payload(payload or {})),
                run_id,
            ),
        )
        seq = cur.fetchone()["seq"]

        assignments = ["status = %s", "updated_at = now()"]
        values: list[Any] = [new_status.value if new_status else None]
        for column, value in run_fields.items():
            assignments.append(f"{column} = %s")
            values.append(value)
        values.append(run_id)
        cur.execute(
            f"UPDATE agent_runs SET {', '.join(assignments)} WHERE id = %s",
            values,
        )

    return seq


def load_events(conn: psycopg.Connection, run_id: int) -> list[Event]:
    """Every event for a run, in order."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT seq, type, payload, created_at FROM agent_events"
            " WHERE run_id = %s ORDER BY seq",
            (run_id,),
        )
        return [
            Event(
                seq=row["seq"],
                type=EventType(row["type"]),
                payload=row["payload"],
                created_at=row["created_at"],
            )
            for row in cur.fetchall()
        ]


def stored_status(conn: psycopg.Connection, run_id: int) -> RunStatus:
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM agent_runs WHERE id = %s", (run_id,))
        row = cur.fetchone()
    if row is None:
        raise LookupError(f"no such run: {run_id}")
    return RunStatus(row["status"])


def verify_replay(conn: psycopg.Connection, run_id: int) -> bool:
    """True when the log and the cached status agree.

    Cheap enough to assert in tests and to run as a periodic audit. If this ever
    returns False in production, the cache is wrong and the log wins.
    """
    return replay_status(load_events(conn, run_id)) == stored_status(conn, run_id)


def count_events(conn: psycopg.Connection, run_id: int, event_type: EventType) -> int:
    """How many events of a type a run already has.

    Draining a transcript uses this as its resume point: the sandbox hands back
    every event from the beginning, and the loop skips the ones already stored.
    That makes the drain idempotent, so a crash part way through costs nothing.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n FROM agent_events WHERE run_id = %s AND type = %s",
            (run_id, event_type.value),
        )
        return cur.fetchone()["n"]


def latest_event(
    conn: psycopg.Connection, run_id: int, event_type: EventType
) -> Event | None:
    """The most recent event of a given type, if any.

    How a restarted orchestrator finds the container it lost: the sandbox handle
    lives in the latest ``sandbox_started`` payload, because a stateless process
    cannot keep it in memory.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT seq, type, payload, created_at FROM agent_events"
            " WHERE run_id = %s AND type = %s"
            " ORDER BY seq DESC LIMIT 1",
            (run_id, event_type.value),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return Event(
        seq=row["seq"],
        type=EventType(row["type"]),
        payload=row["payload"],
        created_at=row["created_at"],
    )


def last_completed_stage(
    conn: psycopg.Connection, run_id: int
) -> dict[str, Any] | None:
    """Payload of the most recent ``stage_completed`` event, if any.

    This is the seam a resumed run picks up from (#12), which is why stages have
    to write structured results rather than prose.
    """
    for event in reversed(load_events(conn, run_id)):
        if event.type is EventType.STAGE_COMPLETED:
            return event.payload
    return None
