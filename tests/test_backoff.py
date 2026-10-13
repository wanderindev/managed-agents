"""Backoff on the abandon path (issue #20).

The gap this closes: a requeued run used to be claimable the moment it was
abandoned, so with 15-second ticks a persistently failing run exhausted all
three attempts in about 45 seconds — turning transient problems (a daemon
mid-restart, a Postgres failover, a five-hour rate limit) into permanent
give-ups. Now an abandoned run keeps its queue position but stays invisible to
dispatch until its ``not_before`` window passes.
"""

from datetime import UTC, datetime, timedelta

import pytest

from orchestrator.enums import EventType, RunStatus
from orchestrator.log import append, create_run, load_events, verify_replay
from orchestrator.loop import Orchestrator, TickResult, backoff_delay
from orchestrator.queue import claim_next_queued, db_now, get_run
from tests.fakes import FakeRunner, VanishingRunner

WORKER = "orchestrator-test"


def no_jitter() -> float:
    return 0.0


def abandon_events(conn, run_id):
    return [e for e in load_events(conn, run_id) if e.type is EventType.RUN_ABANDONED]


# --- the delay itself ---------------------------------------------------------


def test_the_delay_doubles_per_attempt():
    delays = [
        backoff_delay(a, base=60, ceiling=3600, jitter=no_jitter).total_seconds()
        for a in (1, 2, 3, 4)
    ]
    assert delays == [60, 120, 240, 480]


def test_the_delay_respects_the_ceiling():
    capped = backoff_delay(10, base=60, ceiling=600, jitter=no_jitter)
    assert capped.total_seconds() == 600


def test_jitter_shaves_downward_so_the_ceiling_stays_a_ceiling():
    """Spreading retries by adding time would quietly break the cap."""
    full = backoff_delay(1, base=100, ceiling=3600, jitter=lambda: 1.0)
    assert full.total_seconds() == pytest.approx(90)
    at_cap = backoff_delay(10, base=60, ceiling=600, jitter=lambda: 1.0)
    assert at_cap.total_seconds() <= 600


# --- the claim window ---------------------------------------------------------


def _requeue(conn, run_id, not_before):
    append(conn, run_id, EventType.RUN_LEASED, worker_id=WORKER, attempts=1)
    append(
        conn,
        run_id,
        EventType.RUN_ABANDONED,
        {"requeued": True, "reason": "test"},
        worker_id=None,
        lease_expires_at=None,
        not_before=not_before,
    )


def test_a_run_inside_its_window_is_not_claimable(conn):
    run_id = create_run(conn, "smoke", "backoff:1")
    _requeue(conn, run_id, db_now(conn) + timedelta(minutes=5))

    assert get_run(conn, run_id).status is RunStatus.QUEUED
    assert claim_next_queued(conn) is None


def test_the_same_run_is_claimable_once_the_window_passes(conn):
    run_id = create_run(conn, "smoke", "backoff:2")
    _requeue(conn, run_id, db_now(conn) - timedelta(seconds=1))

    claimed = claim_next_queued(conn)
    assert claimed is not None and claimed.id == run_id


def test_not_before_never_affects_the_status_fold(conn):
    """Exactly like lease_expires_at: the window is operational metadata, and
    replaying the log must reproduce the cached status without reading it."""
    run_id = create_run(conn, "smoke", "backoff:3")
    _requeue(conn, run_id, db_now(conn) + timedelta(hours=1))

    assert verify_replay(conn, run_id)


# --- the abandon path ---------------------------------------------------------


def test_abandon_backs_the_run_off_and_says_until_when(conn):
    runner = VanishingRunner()
    orchestrator = Orchestrator(runner, worker_id=WORKER, max_concurrent=1)
    run_id = create_run(conn, "smoke", "backoff:4")
    orchestrator.tick(conn)
    runner.kill_all()

    result = orchestrator.tick(conn)

    assert run_id in result.abandoned
    assert result.leased == [], "the retry waits out the window, not the tick"
    run = get_run(conn, run_id)
    assert run.status is RunStatus.QUEUED
    assert run.not_before > db_now(conn)
    # Stated in the event too, or the log says a run was requeued but not why
    # it then sat still for ten minutes.
    payload = abandon_events(conn, run_id)[-1].payload
    assert datetime.fromisoformat(payload["not_before"]) == run.not_before
    assert verify_replay(conn, run_id)


def test_a_terminal_abandon_gets_no_window(conn):
    """Nothing will ever claim it again, so a wait would be a lie in the log."""
    runner = VanishingRunner()
    orchestrator = Orchestrator(
        runner, worker_id=WORKER, max_concurrent=1, max_attempts=1
    )
    run_id = create_run(conn, "smoke", "backoff:5")
    orchestrator.tick(conn)
    runner.kill_all()
    orchestrator.tick(conn)

    assert get_run(conn, run_id).status is RunStatus.ABANDONED
    assert "not_before" not in abandon_events(conn, run_id)[-1].payload


def test_an_explicit_wait_beats_the_computed_backoff(conn):
    """A known reset time is a fact; the exponential is only a guess."""
    runner = FakeRunner()
    orchestrator = Orchestrator(runner, worker_id=WORKER, max_concurrent=1)
    run_id = create_run(conn, "smoke", "backoff:6")
    orchestrator.tick(conn)
    reset = db_now(conn) + timedelta(hours=4)

    orchestrator._abandon(
        conn, get_run(conn, run_id), "rate limited", TickResult(), not_before=reset
    )

    run = get_run(conn, run_id)
    assert run.not_before == reset
    payload = abandon_events(conn, run_id)[-1].payload
    assert datetime.fromisoformat(payload["not_before"]) == reset


def test_a_rate_limit_reset_becomes_the_wait(conn):
    """The motivating case: the sandbox's stream said exactly when retrying
    stops being useless, so waiting for anything else burns attempts."""
    resets_at = datetime.now(UTC) + timedelta(hours=5)
    runner = VanishingRunner(
        events=[
            {
                "type": "rate_limit_event",
                "rate_limit_info": {
                    "status": "rejected",
                    "rateLimitType": "five_hour",
                    "resetsAt": int(resets_at.timestamp()),
                },
            }
        ]
    )
    orchestrator = Orchestrator(runner, worker_id=WORKER, max_concurrent=1)
    run_id = create_run(conn, "smoke", "backoff:7")
    orchestrator.tick(conn)
    runner.kill_all()

    orchestrator.tick(conn)

    run = get_run(conn, run_id)
    assert run.status is RunStatus.QUEUED
    assert abs((run.not_before - resets_at).total_seconds()) < 1
    assert verify_replay(conn, run_id)


def test_a_rate_limit_banked_by_a_heartbeat_still_becomes_the_wait(conn):
    """With heartbeat drains (#12) the limit is usually stored before the
    container dies, so the GONE-path drain sees nothing new. The log still has
    it, and the abandon must still wait for the stated reset."""
    resets_at = datetime.now(UTC) + timedelta(hours=5)
    runner = VanishingRunner(
        events=[
            {
                "type": "rate_limit_event",
                "rate_limit_info": {
                    "status": "rejected",
                    "rateLimitType": "five_hour",
                    "resetsAt": int(resets_at.timestamp()),
                },
            }
        ]
    )
    orchestrator = Orchestrator(runner, worker_id=WORKER, max_concurrent=1)
    run_id = create_run(conn, "smoke", "backoff:10")
    orchestrator.tick(conn)  # start
    orchestrator.tick(conn)  # heartbeat banks the rate limit
    runner.events = []  # the container dies taking its logs with it
    runner.kill_all()

    orchestrator.tick(conn)

    run = get_run(conn, run_id)
    assert run.status is RunStatus.QUEUED
    assert abs((run.not_before - resets_at).total_seconds()) < 1


def test_a_rate_limit_without_a_usable_reset_falls_back_to_the_guess(conn):
    """A malformed stream event must degrade to the computed backoff, not crash
    the reconcile pass or produce a run with no window at all."""
    runner = VanishingRunner(
        events=[
            {
                "type": "rate_limit_event",
                "rate_limit_info": {"status": "rejected", "resetsAt": "soon"},
            }
        ]
    )
    orchestrator = Orchestrator(runner, worker_id=WORKER, max_concurrent=1)
    run_id = create_run(conn, "smoke", "backoff:9")
    orchestrator.tick(conn)
    runner.kill_all()

    orchestrator.tick(conn)

    run = get_run(conn, run_id)
    assert run.status is RunStatus.QUEUED
    assert run.not_before > db_now(conn)


def test_a_backed_off_run_yields_its_slot_to_newer_work(conn):
    """The bonus fix: requeueing keeps the original queue position, so before
    #20 a run that kept dying retried ahead of newer work every time. Backed
    off, it is invisible to the claim and the newer run goes first."""
    runner = VanishingRunner()
    orchestrator = Orchestrator(runner, worker_id=WORKER, max_concurrent=1)
    older = create_run(conn, "smoke", "backoff:8a")
    orchestrator.tick(conn)
    runner.kill_all()
    newer = create_run(conn, "smoke", "backoff:8b")

    result = orchestrator.tick(conn)

    assert older in result.abandoned
    assert result.leased == [newer], "no head-of-line blocking inside the window"
