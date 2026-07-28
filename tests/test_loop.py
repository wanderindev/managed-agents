"""The orchestrator loop: dispatch, heartbeat, reconcile, and restart safety."""

from datetime import UTC, datetime, timedelta

import pytest

from orchestrator.enums import EventType, RunStatus
from orchestrator.log import append, create_run, load_events, verify_replay
from orchestrator.loop import Orchestrator, TickResult
from orchestrator.queue import get_run
from tests.fakes import FakeRunner, VanishingRunner

WORKER = "orchestrator-test"


@pytest.fixture()
def runner():
    return FakeRunner()


@pytest.fixture()
def lost_runner():
    """A runner whose containers vanish rather than finish.

    Since #5 the loop distinguishes a sandbox that *completed* from one that was
    *lost*, so a test about abandonment has to say which it means. Killing a
    container that then reports SUCCEEDED is a finished run, not a lost one.
    """
    return VanishingRunner()


@pytest.fixture()
def lost(lost_runner):
    # Zero backoff so an abandoned run is claimable again in the same tick.
    # These tests exercise the lease/reconcile machinery; the backoff window
    # itself (#20) has its own suite in test_backoff.py.
    return Orchestrator(
        lost_runner,
        worker_id=WORKER,
        max_concurrent=1,
        lease_seconds=300,
        max_attempts=3,
        backoff_base_seconds=0,
    )


@pytest.fixture()
def orchestrator(runner):
    return Orchestrator(
        runner,
        worker_id=WORKER,
        max_concurrent=1,
        lease_seconds=300,
        max_attempts=3,
        backoff_base_seconds=0,
    )


def _events(conn, run_id):
    return [e.type for e in load_events(conn, run_id)]


# --- dispatch ----------------------------------------------------------------


def test_an_idle_tick_does_nothing(conn, orchestrator):
    assert orchestrator.tick(conn).idle


def test_dispatch_leases_and_starts_the_oldest_run(conn, orchestrator, runner):
    first = create_run(conn, "sentry_triage", "sentry:D-1")
    create_run(conn, "sentry_triage", "sentry:D-2")

    result = orchestrator.tick(conn)

    assert result.leased == [first]
    assert runner.started == [first]
    run = get_run(conn, first)
    assert run.status is RunStatus.RUNNING
    assert run.worker_id == WORKER
    assert run.attempts == 1
    assert run.lease_expires_at > datetime.now(UTC)
    assert _events(conn, first) == [
        EventType.RUN_QUEUED,
        EventType.RUN_LEASED,
        EventType.SANDBOX_STARTED,
    ]
    assert verify_replay(conn, first)


def test_the_handle_is_recoverable_from_the_log(conn, orchestrator, runner):
    """A stateless orchestrator has nowhere else to keep it."""
    run_id = create_run(conn, "sentry_triage", "sentry:D-3")
    orchestrator.tick(conn)

    started = [
        e for e in load_events(conn, run_id) if e.type is EventType.SANDBOX_STARTED
    ]
    assert started[-1].payload["container"] in runner.alive


def test_dispatch_respects_the_concurrency_cap(conn, orchestrator, runner):
    create_run(conn, "sentry_triage", "sentry:D-4")
    create_run(conn, "sentry_triage", "sentry:D-5")

    orchestrator.tick(conn)
    second = orchestrator.tick(conn)

    assert second.leased == []
    assert len(runner.started) == 1


def test_a_higher_cap_dispatches_more_in_one_tick(conn, runner):
    orchestrator = Orchestrator(runner, worker_id=WORKER, max_concurrent=3)
    for n in range(4):
        create_run(conn, "sentry_triage", f"sentry:D-cap-{n}")

    assert len(orchestrator.tick(conn).leased) == 3


def test_a_valid_foreign_lease_consumes_a_slot(conn, orchestrator, runner):
    """The cap counts other workers, or a stale lease lets it be exceeded."""
    foreign = create_run(conn, "sentry_triage", "sentry:D-6")
    append(
        conn,
        foreign,
        EventType.RUN_LEASED,
        worker_id="another-orchestrator",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=10),
    )
    create_run(conn, "sentry_triage", "sentry:D-7")

    result = orchestrator.tick(conn)

    assert result.leased == []
    assert runner.started == []


def test_a_stale_lease_is_freed_and_the_slot_reused_in_one_tick(conn, orchestrator):
    stale = create_run(conn, "sentry_triage", "sentry:D-8")
    append(
        conn,
        stale,
        EventType.RUN_LEASED,
        worker_id="dead-worker",
        lease_expires_at=datetime.now(UTC) - timedelta(minutes=10),
    )
    create_run(conn, "sentry_triage", "sentry:D-9")

    result = orchestrator.tick(conn)

    assert result.abandoned == [stale]
    # Requeueing keeps a run's original queue position, so with this fixture's
    # zero backoff the reclaimed run is still the oldest and goes first. Under a
    # real backoff window it would yield its turn to newer work instead; see
    # test_backoff.py.
    assert result.leased == [stale]


# --- start failures ----------------------------------------------------------


def test_a_sandbox_that_will_not_start_is_requeued(conn):
    runner = FakeRunner(fail_to_start=True)
    orchestrator = Orchestrator(runner, worker_id=WORKER, max_concurrent=1)
    run_id = create_run(conn, "sentry_triage", "sentry:F-1")

    result = orchestrator.tick(conn)

    assert result.leased == [run_id]
    assert result.abandoned == [run_id]
    run = get_run(conn, run_id)
    assert run.status is RunStatus.QUEUED
    assert run.attempts == 1
    assert run.worker_id is None
    assert verify_replay(conn, run_id)


def test_a_start_failure_stops_dispatch_for_the_tick(conn):
    """Do not march the whole queue through the same broken daemon."""
    runner = FakeRunner(fail_to_start=True)
    orchestrator = Orchestrator(runner, worker_id=WORKER, max_concurrent=3)
    create_run(conn, "sentry_triage", "sentry:F-2")
    create_run(conn, "sentry_triage", "sentry:F-3")

    assert len(orchestrator.tick(conn).leased) == 1


def test_repeated_start_failures_eventually_give_up(conn):
    runner = FakeRunner(fail_to_start=True)
    orchestrator = Orchestrator(
        runner,
        worker_id=WORKER,
        max_concurrent=1,
        max_attempts=3,
        backoff_base_seconds=0,
    )
    run_id = create_run(conn, "sentry_triage", "sentry:F-4")

    for _ in range(3):
        orchestrator.tick(conn)

    run = get_run(conn, run_id)
    assert run.attempts == 3
    assert run.status is RunStatus.ABANDONED
    assert verify_replay(conn, run_id)


# --- heartbeat ---------------------------------------------------------------


def test_a_live_sandbox_gets_its_lease_extended(conn, orchestrator):
    run_id = create_run(conn, "sentry_triage", "sentry:HB-1")
    orchestrator.tick(conn)
    before = get_run(conn, run_id).lease_expires_at
    events_before = len(load_events(conn, run_id))

    result = orchestrator.tick(conn)

    assert result.heartbeated == [run_id]
    assert get_run(conn, run_id).lease_expires_at >= before
    # The narrative did not grow. This is why heartbeats are not events.
    assert len(load_events(conn, run_id)) == events_before


def test_a_heartbeat_banks_the_transcript_so_far(conn):
    """The load-bearing half of #12: before this, a killed container took its
    whole history with it and a resume had nothing to read."""
    transcript = [{"type": "system"}, {"type": "assistant", "n": 1}]
    runner = FakeRunner(events=transcript)
    orchestrator = Orchestrator(runner, worker_id=WORKER, max_concurrent=1)
    run_id = create_run(conn, "sentry_triage", "sentry:HB-2")

    orchestrator.tick(conn)  # start
    orchestrator.tick(conn)  # heartbeat drains

    stored = [e for e in load_events(conn, run_id) if e.type is EventType.CLAUDE_EVENT]
    assert [e.payload for e in stored] == transcript


def test_heartbeat_drains_do_not_duplicate_on_finish(conn):
    """finish() hands back the same transcript from the beginning; the
    count-based skip must make the two drains add up to one copy."""
    runner = FakeRunner(events=[{"type": "assistant", "n": 1}])
    orchestrator = Orchestrator(runner, worker_id=WORKER, max_concurrent=1)
    run_id = create_run(conn, "sentry_triage", "sentry:HB-3")

    orchestrator.tick(conn)  # start
    orchestrator.tick(conn)  # heartbeat drains
    runner.kill_all()
    orchestrator.tick(conn)  # finish drains the same events

    stored = [e for e in load_events(conn, run_id) if e.type is EventType.CLAUDE_EVENT]
    assert len(stored) == 1
    assert get_run(conn, run_id).status is RunStatus.DONE


def test_events_drained_at_heartbeat_survive_a_lost_sandbox(conn):
    """A truly vanished container hands back nothing at finish; what the
    heartbeat banked is all a resume gets, and it must still be there."""
    runner = VanishingRunner(events=[{"type": "assistant", "n": 1}])
    orchestrator = Orchestrator(runner, worker_id=WORKER, max_concurrent=1)
    run_id = create_run(conn, "sentry_triage", "sentry:HB-4")

    orchestrator.tick(conn)  # start
    orchestrator.tick(conn)  # heartbeat drains
    runner.events = []  # the container dies taking its logs with it
    runner.kill_all()
    result = orchestrator.tick(conn)

    assert run_id in result.abandoned
    stored = [e for e in load_events(conn, run_id) if e.type is EventType.CLAUDE_EVENT]
    assert [e.payload for e in stored] == [{"type": "assistant", "n": 1}]


def test_a_retrys_transcript_is_not_swallowed_by_its_predecessors(conn):
    """Found live by the run-8 demo: the drain's skip counted the whole run,
    but each container's transcript restarts from zero. A killed attempt that
    banked more events than its retry has emitted made the retry's drain skip
    everything, so the retry finished with no transcript at all — and a third
    attempt would have resumed from the wrong attempt's markers."""
    runner = VanishingRunner(
        events=[{"type": "assistant", "n": 1}, {"type": "assistant", "n": 2}]
    )
    orchestrator = Orchestrator(
        runner, worker_id=WORKER, max_concurrent=1, backoff_base_seconds=0
    )
    run_id = create_run(conn, "sentry_triage", "sentry:HB-6")

    orchestrator.tick(conn)  # start attempt 1
    orchestrator.tick(conn)  # heartbeat banks both events
    runner.events = []  # attempt 1 dies taking its logs with it
    runner.kill_all()
    orchestrator.tick(conn)  # abandon + re-lease + start attempt 2
    runner.events = [{"type": "assistant", "resumed": True}]  # shorter transcript
    orchestrator.tick(conn)  # heartbeat must bank it, not skip it

    stored = [
        e.payload for e in load_events(conn, run_id) if e.type is EventType.CLAUDE_EVENT
    ]
    assert stored == [
        {"type": "assistant", "n": 1},
        {"type": "assistant", "n": 2},
        {"type": "assistant", "resumed": True},
    ]


def test_a_failing_drain_does_not_cost_the_heartbeat(conn, orchestrator, runner):
    """The lease extension commits first: a docker-logs hiccup must not let the
    lease lapse, which would get a healthy run abandoned by the next tick."""
    run_id = create_run(conn, "sentry_triage", "sentry:HB-5")
    orchestrator.tick(conn)
    before = get_run(conn, run_id).lease_expires_at

    def boom(handle):
        raise RuntimeError("docker daemon hiccup")

    runner.logs = boom
    result = orchestrator.tick(conn)

    assert result.heartbeated == [run_id]
    assert get_run(conn, run_id).lease_expires_at >= before


# --- reconcile ---------------------------------------------------------------


def test_a_lost_sandbox_is_requeued(conn, lost_runner):
    """Reconcile in isolation, via a cap of 0. Also how you drain the host."""
    run_id = create_run(conn, "sentry_triage", "sentry:R-1")
    Orchestrator(lost_runner, worker_id=WORKER, max_concurrent=1).tick(conn)
    lost_runner.kill_all()

    draining = Orchestrator(lost_runner, worker_id=WORKER, max_concurrent=0)
    result = draining.tick(conn)

    assert run_id in result.abandoned
    assert result.leased == []
    run = get_run(conn, run_id)
    assert run.status is RunStatus.QUEUED
    assert run.worker_id is None
    assert run.lease_expires_at is None
    assert verify_replay(conn, run_id)


def test_a_lost_sandbox_is_replaced_in_the_same_tick(conn, lost, lost_runner):
    run_id = create_run(conn, "sentry_triage", "sentry:R-1b")
    lost.tick(conn)
    first_handle = lost_runner.alive.pop()
    lost_runner.kill_all()

    result = lost.tick(conn)

    assert result.abandoned == [run_id]
    assert result.leased == [run_id]
    run = get_run(conn, run_id)
    assert run.status is RunStatus.RUNNING
    assert run.attempts == 2
    assert lost_runner.alive and first_handle not in lost_runner.alive, (
        "a fresh container"
    )
    assert verify_replay(conn, run_id)


def test_the_abandon_event_always_states_requeued_explicitly(conn, lost, lost_runner):
    """A missing flag folds to terminal, which would strand a retryable run."""
    run_id = create_run(conn, "sentry_triage", "sentry:R-2")
    lost.tick(conn)
    lost_runner.kill_all()
    lost.tick(conn)

    abandoned = [
        e for e in load_events(conn, run_id) if e.type is EventType.RUN_ABANDONED
    ]
    assert "requeued" in abandoned[-1].payload
    assert abandoned[-1].payload["requeued"] is True
    assert abandoned[-1].payload["reason"]


def test_a_lease_with_no_sandbox_started_is_abandoned(conn, orchestrator):
    """The orchestrator died between committing the lease and launching."""
    run_id = create_run(conn, "sentry_triage", "sentry:R-3")
    append(conn, run_id, EventType.RUN_LEASED, worker_id=WORKER)

    result = orchestrator.tick(conn)

    assert run_id in result.abandoned
    reason = [
        e for e in load_events(conn, run_id) if e.type is EventType.RUN_ABANDONED
    ][-1].payload["reason"]
    assert "never started" in reason


def test_a_foreign_lease_that_is_still_valid_is_left_alone(conn, orchestrator):
    run_id = create_run(conn, "sentry_triage", "sentry:R-4")
    append(
        conn,
        run_id,
        EventType.RUN_LEASED,
        worker_id="another-orchestrator",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=10),
    )

    result = orchestrator.tick(conn)

    assert result.abandoned == []
    assert get_run(conn, run_id).status is RunStatus.LEASED


def test_a_foreign_lease_that_expired_is_reclaimed(conn, orchestrator, runner):
    run_id = create_run(conn, "sentry_triage", "sentry:R-5")
    append(
        conn,
        run_id,
        EventType.RUN_LEASED,
        worker_id="another-orchestrator",
        lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
    )

    result = orchestrator.tick(conn)

    assert run_id in result.abandoned
    # Reclaimed, meaning it is ours now rather than merely released.
    assert result.leased == [run_id]
    assert get_run(conn, run_id).worker_id == WORKER
    assert runner.started == [run_id]


def test_a_finished_run_is_not_reconciled(conn, orchestrator, runner):
    run_id = create_run(conn, "sentry_triage", "sentry:R-6")
    orchestrator.tick(conn)
    append(conn, run_id, EventType.RUN_DONE)
    runner.kill_all()

    result = orchestrator.tick(conn)

    assert result.abandoned == []
    assert get_run(conn, run_id).status is RunStatus.DONE


# --- the acceptance criterion for #4 ----------------------------------------


def test_a_restarted_orchestrator_reattaches_to_a_live_sandbox(conn, runner):
    """No special boot path: the first tick reconciles whatever it inherits."""
    run_id = create_run(conn, "sentry_triage", "sentry:RESTART-1")
    first = Orchestrator(runner, worker_id=WORKER, max_concurrent=1)
    first.tick(conn)
    del first  # the process dies here

    reborn = Orchestrator(runner, worker_id=WORKER, max_concurrent=1)
    result = reborn.tick(conn)

    assert result.heartbeated == [run_id]
    assert result.leased == []
    assert runner.started == [run_id], "the run must not be started twice"
    assert get_run(conn, run_id).status is RunStatus.RUNNING


def test_a_restarted_orchestrator_requeues_a_lost_sandbox_exactly_once(
    conn, lost_runner
):
    """The no-double-PR guarantee: one retry, not one per tick."""
    run_id = create_run(conn, "sentry_triage", "sentry:RESTART-2")
    first = Orchestrator(lost_runner, worker_id=WORKER, max_concurrent=1)
    first.tick(conn)
    lost_runner.kill_all()  # the sandbox is lost with the orchestrator
    del first

    reborn = Orchestrator(
        lost_runner, worker_id=WORKER, max_concurrent=1, backoff_base_seconds=0
    )
    recovery = reborn.tick(conn)

    assert run_id in recovery.abandoned
    assert recovery.leased == [run_id], "recovered in the same tick that freed it"
    assert lost_runner.started == [run_id, run_id]
    run = get_run(conn, run_id)
    assert run.attempts == 2
    assert run.status is RunStatus.RUNNING
    assert verify_replay(conn, run_id)

    abandons = [
        e for e in load_events(conn, run_id) if e.type is EventType.RUN_ABANDONED
    ]
    assert len(abandons) == 1, "requeued once, not once per tick"


def test_a_run_survives_the_full_kill_and_resume_cycle(conn, lost_runner):
    """The #12 shape, driven entirely by the loop."""
    run_id = create_run(conn, "sentry_triage", "sentry:RESUME-1")
    orchestrator = Orchestrator(
        lost_runner, worker_id=WORKER, max_concurrent=1, backoff_base_seconds=0
    )

    orchestrator.tick(conn)
    append(conn, run_id, EventType.STAGE_COMPLETED, {"stage": "investigate"})
    lost_runner.kill_all()
    orchestrator.tick(conn)  # abandons, requeues, and re-leases
    append(conn, run_id, EventType.RUN_DONE)

    assert get_run(conn, run_id).status is RunStatus.DONE
    assert verify_replay(conn, run_id)
    # The work done before the kill is still in the log for the retry to read.
    from orchestrator.log import last_completed_stage

    assert last_completed_stage(conn, run_id) == {"stage": "investigate"}


def test_ticks_are_idempotent_under_repetition(conn, orchestrator, runner):
    create_run(conn, "sentry_triage", "sentry:IDEM-1")
    for _ in range(5):
        orchestrator.tick(conn)
    assert len(runner.started) == 1


# --- run_forever -------------------------------------------------------------


def test_run_forever_stops_when_told(conn, orchestrator, migrated_dsn):
    ticks = []
    orchestrator.run_forever(
        dsn=migrated_dsn,
        should_stop=lambda: len(ticks) >= 3,
        sleep=lambda _: ticks.append(1),
    )
    assert len(ticks) == 3


def test_a_busy_tick_is_reported(orchestrator, caplog):
    """An operator watching the logs has nothing else to go on."""
    result = TickResult(leased=[1], abandoned=[2])
    with caplog.at_level("INFO", logger="orchestrator.loop"):
        orchestrator.report(result)
    assert "leased=[1]" in caplog.text
    assert "abandoned=[2]" in caplog.text


def test_an_idle_tick_is_not_reported(orchestrator, caplog):
    """Otherwise a quiet weekend fills the log with nothing."""
    with caplog.at_level("INFO", logger="orchestrator.loop"):
        orchestrator.report(TickResult())
    assert caplog.text == ""


def test_run_forever_survives_a_failing_tick(conn, orchestrator, monkeypatch):
    """One bad tick must not kill the process."""
    calls = []

    def boom(_conn):
        calls.append(1)
        raise RuntimeError("database went away")

    monkeypatch.setattr(orchestrator, "tick", boom)
    orchestrator.run_forever(
        dsn="postgresql://nobody@127.0.0.1:1/nothing",
        should_stop=lambda: len(calls) >= 2 or len(calls) < 0,
        sleep=lambda _: calls.append(1),
    )
    assert calls, "the loop kept going rather than raising"
