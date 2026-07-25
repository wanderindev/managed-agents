"""Queue reads and the lease write."""

from datetime import UTC, datetime, timedelta

import pytest

from orchestrator.enums import EventType, RunStatus
from orchestrator.log import append, create_run
from orchestrator.queue import (
    active_runs,
    claim_next_queued,
    extend_lease,
    get_run,
    queued_count,
)


def test_claim_next_queued_is_none_on_an_empty_queue(conn):
    assert claim_next_queued(conn) is None


def test_claim_next_queued_takes_the_oldest(conn):
    first = create_run(conn, "sentry_triage", "sentry:Q-1")
    create_run(conn, "sentry_triage", "sentry:Q-2")
    claimed = claim_next_queued(conn)
    assert claimed is not None
    assert claimed.id == first


def test_claim_next_queued_ignores_active_runs(conn):
    run_id = create_run(conn, "sentry_triage", "sentry:Q-3")
    append(conn, run_id, EventType.RUN_LEASED)
    assert claim_next_queued(conn) is None


def test_active_runs_covers_leased_and_running_only(conn):
    leased = create_run(conn, "sentry_triage", "sentry:A-1")
    append(conn, leased, EventType.RUN_LEASED)
    running = create_run(conn, "sentry_triage", "sentry:A-2")
    append(conn, running, EventType.RUN_LEASED)
    append(conn, running, EventType.SANDBOX_STARTED)
    create_run(conn, "sentry_triage", "sentry:A-3")  # stays QUEUED
    done = create_run(conn, "sentry_triage", "sentry:A-4")
    append(conn, done, EventType.RUN_DONE)

    assert [r.id for r in active_runs(conn)] == [leased, running]


def test_active_runs_includes_other_workers(conn):
    """The concurrency cap has to count a stale lease or it can be exceeded."""
    run_id = create_run(conn, "sentry_triage", "sentry:A-5")
    append(conn, run_id, EventType.RUN_LEASED, worker_id="somebody-else")
    assert [r.worker_id for r in active_runs(conn)] == ["somebody-else"]


def test_queued_count(conn):
    create_run(conn, "sentry_triage", "sentry:C-1")
    create_run(conn, "sentry_triage", "sentry:C-2")
    assert queued_count(conn) == 2


def test_lease_expired_treats_a_missing_lease_as_expired(conn):
    run_id = create_run(conn, "sentry_triage", "sentry:E-1")
    run = get_run(conn, run_id)
    assert run.lease_expires_at is None
    assert run.lease_expired(datetime.now(UTC)) is True


def test_lease_expired_respects_a_future_lease(conn):
    run_id = create_run(conn, "sentry_triage", "sentry:E-2")
    future = datetime.now(UTC) + timedelta(minutes=5)
    append(conn, run_id, EventType.RUN_LEASED, lease_expires_at=future)
    run = get_run(conn, run_id)
    assert run.lease_expired(datetime.now(UTC)) is False
    assert run.lease_expired(future + timedelta(seconds=1)) is True


def test_extend_lease_pushes_the_expiry_out(conn):
    run_id = create_run(conn, "sentry_triage", "sentry:H-1")
    past = datetime.now(UTC) - timedelta(minutes=1)
    append(conn, run_id, EventType.RUN_LEASED, lease_expires_at=past)

    new_expiry = extend_lease(conn, run_id, "orchestrator-1", 300)
    assert new_expiry is not None
    assert new_expiry > datetime.now(UTC)
    assert get_run(conn, run_id).lease_expires_at == new_expiry


def test_extend_lease_appends_no_event(conn):
    """A heartbeat every tick would bury the run's actual narrative."""
    run_id = create_run(conn, "sentry_triage", "sentry:H-2")
    append(conn, run_id, EventType.RUN_LEASED)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n FROM agent_events WHERE run_id = %s", (run_id,)
        )
        before = cur.fetchone()["n"]

    extend_lease(conn, run_id, "orchestrator-1", 300)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n FROM agent_events WHERE run_id = %s", (run_id,)
        )
        assert cur.fetchone()["n"] == before


def test_extend_lease_will_not_resurrect_a_terminal_run(conn):
    run_id = create_run(conn, "sentry_triage", "sentry:H-3")
    append(conn, run_id, EventType.RUN_LEASED)
    append(conn, run_id, EventType.RUN_DONE)

    assert extend_lease(conn, run_id, "orchestrator-1", 300) is None
    assert get_run(conn, run_id).status is RunStatus.DONE


def test_get_run_raises_for_an_unknown_id(conn):
    with pytest.raises(LookupError):
        get_run(conn, 999_999)
