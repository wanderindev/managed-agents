"""What the loop makes of a stopped sandbox.

The gap this closes: #4 only knew alive from dead, so a container that finished
successfully would have been abandoned and retried. "Not running any more" covers
three different situations and only one of them deserves a retry.
"""

import pytest

from orchestrator.enums import EventType, Outcome, RunStatus
from orchestrator.log import append, create_run, load_events, verify_replay
from orchestrator.loop import Orchestrator
from orchestrator.queue import get_run
from tests.fakes import FakeRunner

WORKER = "orchestrator-test"


def orchestrate(runner, **kwargs):
    kwargs.setdefault("max_concurrent", 1)
    # Zero backoff so a requeued run is claimable in the same tick; the backoff
    # window itself (#20) is covered in test_backoff.py.
    kwargs.setdefault("backoff_base_seconds", 0)
    return Orchestrator(runner, worker_id=WORKER, **kwargs)


def events_of(conn, run_id, event_type):
    return [e for e in load_events(conn, run_id) if e.type is event_type]


# --- the three ways a container stops ----------------------------------------


def test_a_successful_sandbox_completes_the_run(conn):
    runner = FakeRunner(outcome=Outcome.SUCCEEDED, exit_code=0)
    orchestrator = orchestrate(runner)
    run_id = create_run(conn, "smoke", "smoke:1")
    orchestrator.tick(conn)
    runner.kill_all()

    result = orchestrator.tick(conn)

    assert result.finished == [run_id]
    assert result.abandoned == []
    run = get_run(conn, run_id)
    assert run.status is RunStatus.DONE
    assert run.attempts == 1, "a success must not be retried"
    assert verify_replay(conn, run_id)


def test_a_failed_sandbox_fails_the_run_rather_than_retrying_it(conn):
    """Exit 1 is a verdict on the work. Retrying just repeats it."""
    runner = FakeRunner(outcome=Outcome.FAILED, exit_code=1, stderr="boom")
    orchestrator = orchestrate(runner)
    run_id = create_run(conn, "smoke", "smoke:2")
    orchestrator.tick(conn)
    runner.kill_all()

    orchestrator.tick(conn)

    assert get_run(conn, run_id).status is RunStatus.FAILED
    assert events_of(conn, run_id, EventType.RUN_ABANDONED) == []
    assert verify_replay(conn, run_id)


def test_a_vanished_sandbox_is_abandoned_and_retried(conn):
    """A host reboot loses the work. That one does deserve a retry."""
    runner = FakeRunner(outcome=Outcome.GONE)
    orchestrator = orchestrate(runner)
    run_id = create_run(conn, "smoke", "smoke:3")
    orchestrator.tick(conn)
    runner.kill_all()

    result = orchestrator.tick(conn)

    assert run_id in result.abandoned
    assert get_run(conn, run_id).attempts == 2
    assert verify_replay(conn, run_id)


# --- the stage_completed record ----------------------------------------------


def test_the_stage_record_carries_what_a_human_needs(conn):
    runner = FakeRunner(
        outcome=Outcome.FAILED,
        exit_code=124,
        stderr="sandbox exceeded its 1800s wall clock",
        result={"outcome": "NEEDS_HUMAN"},
    )
    orchestrator = orchestrate(runner)
    run_id = create_run(conn, "smoke", "smoke:4")
    orchestrator.tick(conn)
    runner.kill_all()
    orchestrator.tick(conn)

    stage = events_of(conn, run_id, EventType.STAGE_COMPLETED)[-1].payload
    assert stage["exit_code"] == 124
    assert stage["outcome"] == Outcome.FAILED.value
    assert stage["result"] == {"outcome": "NEEDS_HUMAN"}
    assert "wall clock" in stage["stderr"]


def test_the_structured_result_survives_for_a_resumed_run_to_read(conn):
    """#12 resumes from the last stage, so the stage has to carry data."""
    from orchestrator.log import last_completed_stage

    runner = FakeRunner(result={"outcome": "FIX", "pr": 42})
    orchestrator = orchestrate(runner)
    run_id = create_run(conn, "smoke", "smoke:5")
    orchestrator.tick(conn)
    runner.kill_all()
    orchestrator.tick(conn)

    assert last_completed_stage(conn, run_id)["result"] == {"outcome": "FIX", "pr": 42}


# --- draining the transcript -------------------------------------------------


def test_the_transcript_lands_in_the_log(conn):
    transcript = [
        {"type": "system", "subtype": "init"},
        {"type": "assistant", "message": {"content": "hello"}},
        {"type": "result", "subtype": "success"},
    ]
    runner = FakeRunner(events=transcript)
    orchestrator = orchestrate(runner)
    run_id = create_run(conn, "smoke", "smoke:6")
    orchestrator.tick(conn)
    runner.kill_all()
    orchestrator.tick(conn)

    stored = [e.payload for e in events_of(conn, run_id, EventType.CLAUDE_EVENT)]
    assert stored == transcript


def test_draining_twice_does_not_duplicate(conn):
    """The runner hands back the whole transcript every time on purpose."""
    runner = FakeRunner(events=[{"type": "system"}, {"type": "result"}])
    orchestrator = orchestrate(runner)
    run_id = create_run(conn, "smoke", "smoke:7")
    orchestrator.tick(conn)
    runner.kill_all()
    orchestrator.tick(conn)

    # A second harvest of the same container, as a crashed drain would produce.
    run = get_run(conn, run_id)
    orchestrator._drain(conn, run, list(runner.events))

    assert len(events_of(conn, run_id, EventType.CLAUDE_EVENT)) == 2


def test_a_partial_drain_is_completed_by_the_next_one(conn):
    runner = FakeRunner()
    orchestrator = orchestrate(runner)
    run_id = create_run(conn, "smoke", "smoke:8")
    append(conn, run_id, EventType.RUN_LEASED, worker_id=WORKER)
    run = get_run(conn, run_id)

    orchestrator._drain(conn, run, [{"n": 1}, {"n": 2}])
    orchestrator._drain(conn, run, [{"n": 1}, {"n": 2}, {"n": 3}, {"n": 4}])

    stored = [e.payload["n"] for e in events_of(conn, run_id, EventType.CLAUDE_EVENT)]
    assert stored == [1, 2, 3, 4]


def test_a_rate_limit_is_surfaced_not_buried(conn, caplog):
    """Otherwise it looks like an arbitrary failure at 3am."""
    runner = FakeRunner(
        events=[
            {
                "type": "rate_limit_event",
                "rate_limit_info": {
                    "status": "rejected",
                    "rateLimitType": "five_hour",
                    "resetsAt": 1785000600,
                },
            }
        ]
    )
    orchestrator = orchestrate(runner)
    create_run(conn, "smoke", "smoke:9")
    orchestrator.tick(conn)
    runner.kill_all()

    with caplog.at_level("WARNING", logger="orchestrator.loop"):
        orchestrator.tick(conn)

    assert "rate limit" in caplog.text
    assert "five_hour" in caplog.text


def test_an_allowed_rate_limit_event_is_not_a_warning(conn, caplog):
    """Every healthy run emits one of these. Warning on it would be noise."""
    runner = FakeRunner(
        events=[{"type": "rate_limit_event", "rate_limit_info": {"status": "allowed"}}]
    )
    orchestrator = orchestrate(runner)
    create_run(conn, "smoke", "smoke:10")
    orchestrator.tick(conn)
    runner.kill_all()

    with caplog.at_level("WARNING", logger="orchestrator.loop"):
        orchestrator.tick(conn)

    assert "rate limit" not in caplog.text


# --- payload truncation ------------------------------------------------------


def test_an_enormous_event_is_truncated_but_stays_identifiable(conn):
    """One runaway tool result must not be able to bloat the log."""
    huge = {"type": "user", "name": "Bash", "content": "x" * 200_000}
    runner = FakeRunner(events=[huge])
    orchestrator = orchestrate(runner)
    run_id = create_run(conn, "smoke", "smoke:11")
    orchestrator.tick(conn)
    runner.kill_all()
    orchestrator.tick(conn)

    stored = events_of(conn, run_id, EventType.CLAUDE_EVENT)[0].payload
    assert stored["_truncated"] is True
    assert stored["_original_bytes"] > 200_000
    # Still navigable: you can see what it was without storing all of it.
    assert stored["type"] == "user"
    assert stored["name"] == "Bash"
    assert stored["preview"]


def test_a_normal_event_is_stored_verbatim(conn):
    from orchestrator.log import truncate_payload

    payload = {"type": "assistant", "message": {"content": "short"}}
    assert truncate_payload(payload) is payload


# --- job specs ---------------------------------------------------------------


def test_an_unregistered_kind_fails_loudly(conn):
    from orchestrator import jobs

    run = get_run(conn, create_run(conn, "not_a_real_kind", "x:1"))
    with pytest.raises(RuntimeError, match="no job spec registered"):
        jobs.build_spec(run)


def test_the_smoke_job_asks_for_a_structured_result(conn):
    """It exercises the result contract, not just "the agent replied"."""
    from orchestrator import jobs

    run = get_run(conn, create_run(conn, "smoke", "smoke:spec"))
    spec = jobs.build_spec(run)
    assert "result.json" in spec.prompt
    assert spec.repo == ""
