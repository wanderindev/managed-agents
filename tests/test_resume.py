"""Kill-and-resume (#12): stage markers out of the transcript, context back in.

The contract under test: multi-phase prompts instruct the agent to print
``STAGE_COMPLETED {json}`` lines as milestones become true; those lines ride
the stream-json transcript, which heartbeat drains make durable; and a retry's
spec mechanically prepends what the interrupted attempt established — no LLM
involved in the extraction.
"""

from dataclasses import replace

import pytest

from orchestrator import jobs
from orchestrator.log import create_run
from orchestrator.queue import get_run
from orchestrator.sources import sentry
from tests.test_triage import PAYLOAD, FakeDetailClient


@pytest.fixture()
def detail(monkeypatch):
    client = FakeDetailClient()
    monkeypatch.setattr(jobs, "_sentry_client", lambda: client)
    return client


def assistant(text):
    return {
        "type": "assistant",
        "message": {"content": [{"type": "text", "text": text}]},
    }


def triage_run(conn, attempts=1, subject="sentry:RES-1"):
    run = get_run(conn, create_run(conn, sentry.RUN_KIND, subject, PAYLOAD))
    return replace(run, attempts=attempts)


MARKED = [
    {"type": "system", "subtype": "init"},
    assistant(
        "Reading the trace now.\n"
        'STAGE_COMPLETED {"stage": "investigated", "outcome": "FIX",'
        ' "cause": "off-by-one in the sweeper"}'
    ),
    # A tool result that happens to contain a marker (an agent cat-ing its own
    # prompt does exactly this). It must not count as a milestone.
    {
        "type": "user",
        "message": {
            "content": [
                {
                    "type": "tool_result",
                    "content": 'STAGE_COMPLETED {"stage": "pr_opened",'
                    ' "pr_url": "https://example.invalid/quoted"}',
                }
            ]
        },
    },
    assistant('STAGE_COMPLETED {"stage": "pushed", "branch": "agent/run-9"}'),
    {
        "type": "result",
        "result": 'STAGE_COMPLETED {"stage": "investigated", "outcome": "FIX",'
        ' "cause": "refined: the sweeper never bounds its window"}',
    },
]


# --- extraction ---------------------------------------------------------------


def test_markers_come_only_from_the_agents_own_words():
    stages = jobs.stage_markers(MARKED)
    assert [s["stage"] for s in stages] == ["pushed", "investigated"]
    assert all(s.get("pr_url") != "https://example.invalid/quoted" for s in stages)


def test_a_redone_stage_supersedes_its_earlier_claim():
    stages = jobs.stage_markers(MARKED)
    investigated = [s for s in stages if s["stage"] == "investigated"]
    assert len(investigated) == 1
    assert "refined" in investigated[0]["cause"]


def test_malformed_markers_are_skipped_not_fatal():
    events = [
        assistant("STAGE_COMPLETED not json at all"),
        assistant('STAGE_COMPLETED {"no_stage_key": true}'),
        assistant('STAGE_COMPLETED {"stage": "investigated", "outcome": "FIX"}'),
    ]
    assert [s["stage"] for s in jobs.stage_markers(events)] == ["investigated"]


def test_an_unmarked_transcript_yields_nothing():
    assert jobs.stage_markers([assistant("just prose"), {"type": "system"}]) == []


# --- the prompts instruct the markers -----------------------------------------


def test_the_triage_prompt_defines_the_markers(conn, detail):
    run = triage_run(conn)
    prompt = jobs.build_spec(run).prompt
    assert "# Durability markers (MANDATORY)" in prompt
    # Interpolated to single braces and the run's real branch, so the agent can
    # copy the line as-is.
    assert (
        f'STAGE_COMPLETED {{"stage": "pushed", "branch": "agent/run-{run.id}"}}'
        in prompt
    )


def test_the_revision_prompt_defines_the_markers(conn, detail):
    payload = dict(
        PAYLOAD,
        branch="agent/run-77",
        pr_url="https://github.com/x/y/pull/1",
        round=2,
        refutation="the test is tautological",
    )
    run = get_run(conn, create_run(conn, jobs.REVISION_KIND, "sentry:RES-2", payload))
    prompt = jobs.build_spec(run).prompt
    assert "# Durability markers (MANDATORY)" in prompt
    assert 'STAGE_COMPLETED {"stage": "pushed", "branch": "agent/run-77"}' in prompt


# --- the retry's spec ----------------------------------------------------------


def test_a_first_attempt_never_reads_history(conn, detail, monkeypatch):
    reads = []
    monkeypatch.setattr(jobs, "_prior_claude_events", lambda run: reads.append(run.id))
    jobs.build_spec(triage_run(conn, attempts=1))
    assert reads == []


def test_a_retry_is_briefed_on_what_the_predecessor_established(
    conn, detail, monkeypatch
):
    monkeypatch.setattr(jobs, "_prior_claude_events", lambda run: list(MARKED))
    run = triage_run(conn, attempts=2)

    spec = jobs.build_spec(run)

    assert spec.prompt.startswith("# RESUME — this is attempt 2")
    assert '"stage": "pushed"' in spec.prompt
    assert "refined: the sweeper never bounds its window" in spec.prompt
    # The original brief still follows in full.
    assert "PIC-PYTHON-FASTAPI-1Q" in spec.prompt
    assert "/work/result.json" in spec.prompt


def test_a_pushed_stage_makes_the_retry_reuse_the_branch(conn, detail, monkeypatch):
    """The whole point of the seam: the predecessor's pushed commits must not be
    erased by the fresh workspace's checkout -B from origin/main."""
    monkeypatch.setattr(jobs, "_prior_claude_events", lambda run: list(MARKED))
    assert jobs.build_spec(triage_run(conn, attempts=2)).reuse_branch is True


def test_an_unpushed_retry_still_cuts_a_fresh_branch(conn, detail, monkeypatch):
    events = [assistant('STAGE_COMPLETED {"stage": "investigated", "outcome": "FIX"}')]
    monkeypatch.setattr(jobs, "_prior_claude_events", lambda run: events)
    spec = jobs.build_spec(triage_run(conn, attempts=2))
    assert spec.reuse_branch is False
    assert spec.prompt.startswith("# RESUME")


def test_a_markerless_history_changes_nothing(conn, detail, monkeypatch):
    monkeypatch.setattr(
        jobs, "_prior_claude_events", lambda run: [assistant("died early")]
    )
    spec = jobs.build_spec(triage_run(conn, attempts=2))
    assert not spec.prompt.startswith("# RESUME")
    assert spec.reuse_branch is False


def test_a_history_read_failure_falls_back_to_a_plain_spec(conn, detail, monkeypatch):
    """A resume feature must never become a new way to lose the run."""

    def boom(run):
        raise RuntimeError("database went away")

    monkeypatch.setattr(jobs, "_prior_claude_events", boom)
    spec = jobs.build_spec(triage_run(conn, attempts=2))
    assert not spec.prompt.startswith("# RESUME")


def test_a_repoless_job_never_reuses_a_branch(conn, monkeypatch):
    """A pushed marker in a smoke transcript (there should never be one) must
    not flip reuse_branch on a spec with no repo to fetch from."""
    monkeypatch.setattr(
        jobs,
        "_prior_claude_events",
        lambda run: [assistant('STAGE_COMPLETED {"stage": "pushed"}')],
    )
    run = replace(get_run(conn, create_run(conn, "smoke", "smoke:RES-3")), attempts=2)
    spec = jobs.build_spec(run)
    assert spec.reuse_branch is False
    assert spec.prompt.startswith("# RESUME")
