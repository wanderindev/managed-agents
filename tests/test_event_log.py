"""The log's invariants: append-only, per-run ordering, open-subject uniqueness,
and the replay property that issue #3 exists to establish.
"""

from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from orchestrator.enums import TERMINAL_STATUSES, EventType, RunStatus
from orchestrator.log import (
    append,
    apply_event,
    create_run,
    last_completed_stage,
    load_events,
    replay_status,
    stored_status,
    verify_replay,
)


def _execute(conn, sql, params=()):
    """Fire raw SQL, so the ``raises`` + savepoint tests stay one line deep."""
    with conn.cursor() as cur:
        cur.execute(sql, params)


# --- append-only enforcement -------------------------------------------------


def test_events_cannot_be_updated(conn):
    run_id = create_run(conn, "sentry_triage", "sentry:ABC-1")
    with pytest.raises(psycopg.errors.RestrictViolation), conn.transaction():
        _execute(
            conn,
            "UPDATE agent_events SET type = 'tampered' WHERE run_id = %s",
            (run_id,),
        )


def test_events_cannot_be_deleted(conn):
    run_id = create_run(conn, "sentry_triage", "sentry:ABC-2")
    with pytest.raises(psycopg.errors.RestrictViolation), conn.transaction():
        _execute(conn, "DELETE FROM agent_events WHERE run_id = %s", (run_id,))


def test_events_cannot_be_truncated(conn):
    create_run(conn, "sentry_triage", "sentry:ABC-3")
    with pytest.raises(psycopg.errors.RestrictViolation), conn.transaction():
        _execute(conn, "TRUNCATE agent_events")


def test_deleting_a_run_is_restricted_while_it_has_events(conn):
    """History must not disappear as a side effect of removing a run."""
    run_id = create_run(conn, "sentry_triage", "sentry:ABC-4")
    with pytest.raises(psycopg.errors.ForeignKeyViolation), conn.transaction():
        _execute(conn, "DELETE FROM agent_runs WHERE id = %s", (run_id,))


# --- sequencing --------------------------------------------------------------


def test_seq_starts_at_one_and_increments(conn):
    run_id = create_run(conn, "sentry_triage", "sentry:SEQ-1")
    assert append(conn, run_id, EventType.RUN_LEASED) == 2
    assert append(conn, run_id, EventType.SANDBOX_STARTED) == 3
    assert [e.seq for e in load_events(conn, run_id)] == [1, 2, 3]


def test_seq_is_per_run_not_global(conn):
    first = create_run(conn, "sentry_triage", "sentry:SEQ-2")
    second = create_run(conn, "sentry_triage", "sentry:SEQ-3")
    append(conn, first, EventType.RUN_LEASED)
    assert append(conn, second, EventType.RUN_LEASED) == 2


def test_duplicate_seq_is_rejected(conn):
    run_id = create_run(conn, "sentry_triage", "sentry:SEQ-4")
    with pytest.raises(psycopg.errors.UniqueViolation), conn.transaction():
        _execute(
            conn,
            "INSERT INTO agent_events (run_id, seq, type) VALUES (%s, 1, %s)",
            (run_id, EventType.ARTIFACT.value),
        )


def test_append_to_unknown_run_raises(conn):
    with pytest.raises(LookupError):
        append(conn, 999_999, EventType.RUN_LEASED)


# --- open-subject uniqueness (#6's dedup backstop) ---------------------------


def test_duplicate_open_subject_is_rejected(conn):
    create_run(conn, "sentry_triage", "sentry:DUP-1")
    with pytest.raises(psycopg.errors.UniqueViolation), conn.transaction():
        create_run(conn, "sentry_triage", "sentry:DUP-1")


def test_same_subject_under_a_different_kind_is_allowed(conn):
    create_run(conn, "sentry_triage", "sentry:DUP-2")
    assert create_run(conn, "pr_revision", "sentry:DUP-2")


@pytest.mark.parametrize(
    ("event_type", "payload"),
    [
        (EventType.RUN_DONE, None),
        (EventType.RUN_FAILED, None),
        (EventType.RUN_ABANDONED, {"requeued": False}),
    ],
)
def test_a_terminal_run_frees_its_subject(conn, event_type, payload):
    """The same Sentry issue can legitimately come back later."""
    subject = f"sentry:REOPEN-{event_type.value}"
    first = create_run(conn, "sentry_triage", subject)
    append(conn, first, event_type, payload)
    assert stored_status(conn, first) in TERMINAL_STATUSES
    assert create_run(conn, "sentry_triage", subject) != first


def test_a_requeued_run_does_not_free_its_subject(conn):
    run_id = create_run(conn, "sentry_triage", "sentry:REQ-1")
    append(conn, run_id, EventType.RUN_ABANDONED, {"requeued": True})
    assert stored_status(conn, run_id) is RunStatus.QUEUED
    with pytest.raises(psycopg.errors.UniqueViolation), conn.transaction():
        create_run(conn, "sentry_triage", "sentry:REQ-1")


# --- the fold ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("event_type", "payload", "expected"),
    [
        (EventType.RUN_QUEUED, None, RunStatus.QUEUED),
        (EventType.RUN_LEASED, None, RunStatus.LEASED),
        (EventType.SANDBOX_STARTED, None, RunStatus.RUNNING),
        (EventType.HUMAN_GATE, None, RunStatus.AWAITING_HUMAN),
        (EventType.RUN_FAILED, None, RunStatus.FAILED),
        (EventType.RUN_DONE, None, RunStatus.DONE),
        (EventType.RUN_ABANDONED, {"requeued": True}, RunStatus.QUEUED),
        (EventType.RUN_ABANDONED, {"requeued": False}, RunStatus.ABANDONED),
        (EventType.RUN_ABANDONED, None, RunStatus.ABANDONED),
    ],
)
def test_apply_event_transitions(event_type, payload, expected):
    assert apply_event(RunStatus.RUNNING, event_type, payload) is expected


@pytest.mark.parametrize(
    "event_type",
    [EventType.CLAUDE_EVENT, EventType.STAGE_COMPLETED, EventType.ARTIFACT],
)
def test_narrative_events_do_not_move_the_status(event_type):
    assert apply_event(RunStatus.RUNNING, event_type) is RunStatus.RUNNING


def test_replay_of_no_events_is_none():
    assert replay_status([]) is None


# --- the acceptance criterion for #3 ----------------------------------------


def test_replay_reproduces_the_stored_status_through_a_full_lifecycle(conn):
    """Replaying events must reproduce agent_runs.status without reading it."""
    run_id = create_run(conn, "sentry_triage", "sentry:LIFE-1")
    lifecycle = [
        (EventType.RUN_LEASED, {"worker": "orchestrator-1"}),
        (EventType.SANDBOX_STARTED, {"container": "abc123"}),
        (EventType.CLAUDE_EVENT, {"kind": "tool_use", "name": "Read"}),
        (EventType.STAGE_COMPLETED, {"stage": "investigate", "exit_code": 0}),
        (EventType.CLAUDE_EVENT, {"kind": "tool_use", "name": "Edit"}),
        (EventType.STAGE_COMPLETED, {"stage": "patch", "exit_code": 0}),
        (EventType.ARTIFACT, {"pr": 42}),
        (EventType.HUMAN_GATE, {"reason": "awaiting review"}),
        (EventType.RUN_DONE, {"merged": True}),
    ]
    for event_type, payload in lifecycle:
        append(conn, run_id, event_type, payload)
        # Checked after *every* append, so a drift is caught at the event that
        # caused it rather than at the end.
        assert verify_replay(conn, run_id), f"drift after {event_type.value}"

    assert stored_status(conn, run_id) is RunStatus.DONE


def test_replay_survives_a_kill_and_requeue(conn):
    """The #12 shape: abandoned mid-run, requeued, leased again, finished."""
    run_id = create_run(conn, "sentry_triage", "sentry:LIFE-2")
    for event_type, payload in [
        (EventType.RUN_LEASED, None),
        (EventType.SANDBOX_STARTED, None),
        (EventType.STAGE_COMPLETED, {"stage": "investigate", "finding": "off-by-one"}),
        (EventType.RUN_ABANDONED, {"requeued": True, "reason": "lease expired"}),
        (EventType.RUN_LEASED, None),
        (EventType.SANDBOX_STARTED, None),
        (EventType.RUN_DONE, None),
    ]:
        append(conn, run_id, event_type, payload)
        assert verify_replay(conn, run_id)

    assert stored_status(conn, run_id) is RunStatus.DONE


# --- run field writes --------------------------------------------------------


def test_lease_fields_land_with_the_event(conn):
    run_id = create_run(conn, "sentry_triage", "sentry:LEASE-1")
    expires = datetime.now(UTC) + timedelta(minutes=10)
    append(
        conn,
        run_id,
        EventType.RUN_LEASED,
        {"worker": "orchestrator-1"},
        worker_id="orchestrator-1",
        lease_expires_at=expires,
        attempts=1,
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, worker_id, lease_expires_at, attempts"
            " FROM agent_runs WHERE id = %s",
            (run_id,),
        )
        row = cur.fetchone()
    assert row["status"] == RunStatus.LEASED.value
    assert row["worker_id"] == "orchestrator-1"
    assert row["lease_expires_at"] == expires
    assert row["attempts"] == 1


def test_unknown_run_field_is_rejected(conn):
    """The keys are interpolated into SQL, so the whitelist is load-bearing."""
    run_id = create_run(conn, "sentry_triage", "sentry:LEASE-2")
    with pytest.raises(ValueError, match="not a writable agent_runs column"):
        append(conn, run_id, EventType.RUN_LEASED, status="DONE")


def test_negative_attempts_is_rejected(conn):
    run_id = create_run(conn, "sentry_triage", "sentry:LEASE-3")
    with pytest.raises(psycopg.errors.CheckViolation), conn.transaction():
        append(conn, run_id, EventType.RUN_LEASED, attempts=-1)


# --- resume seam -------------------------------------------------------------


def test_last_completed_stage_returns_the_most_recent(conn):
    run_id = create_run(conn, "sentry_triage", "sentry:STAGE-1")
    append(conn, run_id, EventType.STAGE_COMPLETED, {"stage": "investigate"})
    append(conn, run_id, EventType.CLAUDE_EVENT, {"kind": "text"})
    append(conn, run_id, EventType.STAGE_COMPLETED, {"stage": "patch"})
    assert last_completed_stage(conn, run_id) == {"stage": "patch"}


def test_last_completed_stage_is_none_before_any_stage(conn):
    run_id = create_run(conn, "sentry_triage", "sentry:STAGE-2")
    assert last_completed_stage(conn, run_id) is None


def test_stored_status_of_unknown_run_raises(conn):
    with pytest.raises(LookupError):
        stored_status(conn, 999_999)
