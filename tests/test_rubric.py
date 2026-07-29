"""The rubric verifier (#14): rubric at plan time, fresh grading, bounded loop.

Three rules under test: the rubric is written before any output exists and
rides the payload unchanged through the whole chain; the grader gets the
rubric and the artifact and nothing else, defaulting to fail; and the
grade-revise loop is hard-bounded with every grading pass durable as its own
run, so the score trajectory is a query rather than a memory.
"""

from dataclasses import replace

import pytest

from orchestrator import jobs, notify, research_gate
from orchestrator.enums import EventType, RunStatus
from orchestrator.log import append, create_run, load_events
from orchestrator.queue import get_run

TOPIC = "The Panama Railroad and the 1849 gold rush"
SUBTOPICS = ["Financing and construction", "Labor and mortality", "Freight economics"]


_slugs = iter(range(1000))


def write_run(conn, slug=None):
    # The test database persists across tests, and the partial unique index on
    # (kind, subject) means every test needs its own subject — the suite-wide
    # convention (sentry:D-1, sentry:HB-2, ...).
    #
    # Also: open the test's implicit outer transaction BEFORE enqueue, so its
    # `with conn.transaction():` nests as a savepoint the fixture can roll
    # back, instead of being the outermost block and committing leftovers that
    # poison later tests' claim-order and count assertions.
    conn.execute("SELECT 1")
    slug = slug or f"railroad-{next(_slugs)}"
    run_id = research_gate.enqueue(
        conn, topic=TOPIC, subtopics=SUBTOPICS, repo="feliu-dev", slug=slug
    )
    return get_run(conn, run_id)


def chain_next(conn, run, result):
    """Apply followups the way the loop would and return the enqueued run.

    The source run goes DONE first, as it would in production — followups fire
    on completed runs, and the open-subject unique index rightly refuses two
    live runs of one kind for the same subject.
    """
    append(conn, run.id, EventType.RUN_DONE)
    decision = jobs.followups(run, result)
    assert len(decision.enqueue) == 1, decision
    new = decision.enqueue[0]
    return get_run(conn, create_run(conn, new.kind, new.subject, new.payload))


def verdict(id_, ok, gap=None):
    return {
        "id": id_,
        "pass": ok,
        "evidence": f"measured {id_}",
        "gap": gap,
    }


# --- planning time --------------------------------------------------------------


def test_the_rubric_is_written_at_planning_time(conn):
    run = write_run(conn, slug="railroad-plan")
    assert run.kind == jobs.RESEARCH_WRITE_KIND
    assert run.subject == "research-gate:railroad-plan"
    assert [c["id"] for c in run.payload["rubric"]] == [
        "word_count",
        "references",
        "subtopic_coverage",
    ]
    assert run.payload["artifact_path"] == (
        "docs/experiments/research-gate-railroad-plan.md"
    )


def test_a_run_without_a_rubric_fails_the_launch(conn):
    run_id = create_run(
        conn,
        jobs.RESEARCH_WRITE_KIND,
        "research-gate:bare",
        {"repo": "feliu-dev", "artifact_path": "docs/x.md", "topic": "t"},
    )
    with pytest.raises(RuntimeError, match="no rubric"):
        jobs.build_spec(get_run(conn, run_id))


# --- the specs ------------------------------------------------------------------


def test_the_writer_sees_the_rubric_and_pushes_a_branch(conn):
    run = write_run(conn, slug="railroad-spec")
    spec = jobs.build_spec(run)
    assert spec.repo == "feliu-dev"
    assert spec.branch == f"agent/run-{run.id}"
    assert spec.reuse_branch is False
    assert spec.needs_docker is False
    prompt = spec.prompt
    assert "at least 4,000 words" in prompt
    assert "Labor and mortality" in prompt
    assert "docs/experiments/research-gate-railroad-spec.md" in prompt
    assert "Never open a pull request" in prompt
    assert '"outcome": "WROTE" | "NEEDS_HUMAN"' in prompt


def test_the_grader_gets_the_rubric_and_the_artifact_and_nothing_else(conn):
    run = write_run(conn)
    verify = chain_next(conn, replace(run, attempts=1), {"outcome": "WROTE"})
    spec = jobs.build_spec(verify)
    assert spec.reuse_branch is True, "the grader reads the pushed branch"
    prompt = spec.prompt
    assert "grading pass 1 of at most 2" in prompt
    assert "none of its reasoning" in prompt
    assert "The default is fail" in prompt
    assert "Read-only" in prompt
    assert "compute, do not estimate" in prompt
    # The topic brief is the writer's context, not the grader's.
    assert TOPIC not in prompt


def test_the_reviser_gets_only_the_gaps(conn):
    run = write_run(conn)
    verify = chain_next(conn, replace(run, attempts=1), {"outcome": "WROTE"})
    revise = chain_next(
        conn,
        verify,
        {
            "passed": False,
            "verdicts": [
                verdict("word_count", False, gap="3,120 of 4,000 words"),
                verdict("references", True),
            ],
        },
    )
    spec = jobs.build_spec(revise)
    assert spec.reuse_branch is True
    assert "3,120 of 4,000 words" in spec.prompt
    assert "do not rewrite what" in spec.prompt


# --- the chain ------------------------------------------------------------------


def test_the_chain_carries_the_rubric_unchanged_to_the_bound(conn):
    run = write_run(conn)
    rubric = run.payload["rubric"]

    verify1 = chain_next(conn, replace(run, attempts=1), {"outcome": "WROTE"})
    assert verify1.kind == jobs.RUBRIC_VERIFY_KIND
    assert verify1.payload["iteration"] == 1
    assert verify1.payload["branch"] == f"agent/run-{run.id}"
    assert verify1.payload["rubric"] == rubric

    revise = chain_next(
        conn,
        verify1,
        {"passed": False, "verdicts": [verdict("word_count", False, gap="short")]},
    )
    assert revise.kind == jobs.RESEARCH_REVISE_KIND
    assert revise.payload["rubric"] == rubric

    verify2 = chain_next(conn, revise, {"outcome": "REVISED"})
    assert verify2.kind == jobs.RUBRIC_VERIFY_KIND
    assert verify2.payload["iteration"] == 2
    assert verify2.payload["branch"] == f"agent/run-{run.id}", (
        "every pass grades the same branch"
    )


def test_a_clean_grade_parks_for_the_human(conn):
    run = write_run(conn)
    verify = chain_next(conn, replace(run, attempts=1), {"outcome": "WROTE"})
    decision = jobs.followups(
        verify,
        {
            "passed": True,
            "verdicts": [
                verdict("word_count", True),
                verdict("references", True),
                verdict("subtopic_coverage", True),
            ],
            "summary": "clears the bar",
        },
    )
    assert decision.enqueue == ()
    assert decision.human_gate["why"] == "rubric cleared on grading pass 1"


def test_a_pass_inconsistent_with_its_verdicts_is_downgraded(conn):
    """The #8 rule again: a verdict must agree with the findings it is paired
    with, so passed=true alongside a failing criterion does not pass."""
    run = write_run(conn)
    verify = chain_next(conn, replace(run, attempts=1), {"outcome": "WROTE"})
    decision = jobs.followups(
        verify,
        {
            "passed": True,
            "verdicts": [verdict("word_count", False, gap="short")],
        },
    )
    assert decision.human_gate is None
    assert decision.enqueue[0].kind == jobs.RESEARCH_REVISE_KIND


def test_a_verdictless_pass_is_not_a_pass(conn):
    run = write_run(conn)
    verify = chain_next(conn, replace(run, attempts=1), {"outcome": "WROTE"})
    decision = jobs.followups(verify, {"passed": True, "verdicts": []})
    assert decision.human_gate is None or "cleared" not in decision.human_gate["why"]


def test_the_bound_ends_the_loop_with_the_gaps_stated(conn):
    run = write_run(conn)
    verify = chain_next(conn, replace(run, attempts=1), {"outcome": "WROTE"})
    revise = chain_next(
        conn,
        verify,
        {"passed": False, "verdicts": [verdict("word_count", False, gap="short")]},
    )
    verify2 = chain_next(conn, revise, {"outcome": "REVISED"})

    decision = jobs.followups(
        verify2,
        {
            "passed": False,
            "verdicts": [verdict("word_count", False, gap="still 3,800 of 4,000")],
        },
    )
    assert decision.enqueue == ()
    assert decision.human_gate["why"] == "rubric not cleared after 2 grading pass(es)"
    assert decision.human_gate["flagged"][0]["claim"] == "still 3,800 of 4,000"


def test_the_trajectory_is_a_query(conn):
    """Every grading pass is its own run; 'cleared on pass 2' is countable."""
    run = write_run(conn)
    verify1 = chain_next(conn, replace(run, attempts=1), {"outcome": "WROTE"})
    revise = chain_next(
        conn,
        verify1,
        {"passed": False, "verdicts": [verdict("word_count", False, gap="short")]},
    )
    chain_next(conn, revise, {"outcome": "REVISED"})

    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n FROM agent_runs WHERE kind = %s AND subject = %s",
            (jobs.RUBRIC_VERIFY_KIND, run.subject),
        )
        assert cur.fetchone()["n"] == 2


# --- the emails -----------------------------------------------------------------


def _finish(conn, run, result, decision):
    append(
        conn,
        run.id,
        EventType.STAGE_COMPLETED,
        {"outcome": "SUCCEEDED", "exit_code": 0, "result": result},
    )
    append(conn, run.id, EventType.RUN_DONE)
    if decision.human_gate:
        append(conn, run.id, EventType.HUMAN_GATE, decision.human_gate)


def test_a_cleared_rubric_emails_once_and_the_chain_stays_quiet(conn):
    run = write_run(conn)
    write_result = {"outcome": "WROTE", "artifact_path": "docs/x.md"}
    _finish(conn, run, write_result, jobs.Followups())
    verify = chain_next(conn, replace(run, attempts=1), write_result)
    result = {
        "passed": True,
        "verdicts": [verdict("word_count", True)],
        "summary": "clears the bar",
    }
    _finish(conn, verify, result, jobs.followups(verify, result))

    sent = []
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 1
    assert "rubric cleared on grading pass 1" in sent[0].body
    assert get_run(conn, verify.id).status is RunStatus.AWAITING_HUMAN
    # The write run was examined and deliberately suppressed, never skipped.
    write_events = [
        e for e in load_events(conn, run.id) if e.type is EventType.EMAIL_SENT
    ]
    assert write_events and "suppressed" in write_events[0].payload
