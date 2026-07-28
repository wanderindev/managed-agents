"""The dreaming job (#15): digest, enqueue, spec, chain, and its emails.

The invariants that matter: the digest is distilled facts and never raw
transcript, one dream per repo per day ever, flags are never auto-applied,
and neither a finding nor a clean pass is ever silent.
"""

from orchestrator import dream, jobs, notify
from orchestrator.enums import EventType, RunStatus
from orchestrator.log import append, create_run
from orchestrator.queue import get_run
from orchestrator.sources import sentry


def assistant(text):
    return {
        "type": "assistant",
        "message": {"content": [{"type": "text", "text": text}]},
    }


def triage_run(conn, subject, repo="feliu-dev", *, result=None, stages=(), gate=None):
    run_id = create_run(conn, sentry.RUN_KIND, subject, {"repo": repo})
    append(conn, run_id, EventType.RUN_LEASED, worker_id="w", attempts=1)
    append(conn, run_id, EventType.SANDBOX_STARTED, {"container": f"c-{run_id}"})
    for marker in stages:
        append(
            conn,
            run_id,
            EventType.CLAUDE_EVENT,
            assistant(f"STAGE_COMPLETED {marker}"),
        )
    append(
        conn,
        run_id,
        EventType.STAGE_COMPLETED,
        {"outcome": "SUCCEEDED", "exit_code": 0, "result": result},
    )
    append(conn, run_id, EventType.RUN_DONE, worker_id=None, lease_expires_at=None)
    if gate:
        append(conn, run_id, EventType.HUMAN_GATE, gate)
    return run_id


# --- the digest ---------------------------------------------------------------


def test_the_digest_distills_results_stages_and_gates(conn):
    run_id = triage_run(
        conn,
        "sentry:DRM-1",
        result={"outcome": "NEEDS_HUMAN", "reason": "pool starvation"},
        stages=('{"stage": "investigated", "outcome": "NEEDS_HUMAN"}',),
        gate={"why": "needs a human"},
    )

    entries = dream.digest(conn, "feliu-dev", days=7)

    assert [e["run"] for e in entries] == [run_id]
    entry = entries[0]
    assert entry["result"]["outcome"] == "NEEDS_HUMAN"
    assert entry["stages"] == [{"stage": "investigated", "outcome": "NEEDS_HUMAN"}]
    assert entry["gate"] == {"why": "needs a human"}
    assert entry["status"] == "AWAITING_HUMAN"


def test_the_digest_is_scoped_to_the_repo(conn):
    triage_run(conn, "sentry:DRM-2a", repo="feliu-dev")
    triage_run(conn, "sentry:DRM-2b", repo="panama-in-context")

    entries = dream.digest(conn, "feliu-dev", days=7)

    assert [e["subject"] for e in entries] == ["sentry:DRM-2a"]


def test_the_digest_caps_runs_and_clips_fields(conn):
    for n in range(dream.MAX_DIGEST_RUNS + 3):
        triage_run(conn, f"sentry:DRM-3-{n}", result={"summary": "x" * 2000})

    entries = dream.digest(conn, "feliu-dev", days=7)

    assert len(entries) == dream.MAX_DIGEST_RUNS
    assert all(len(e["result"]["summary"]) < 700 for e in entries)


def test_recent_repos_lists_active_repos(conn):
    triage_run(conn, "sentry:DRM-4a", repo="feliu-dev")
    triage_run(conn, "sentry:DRM-4b", repo="panama-in-context")
    assert dream.recent_repos(conn, days=7) == ["feliu-dev", "panama-in-context"]


# --- enqueueing ---------------------------------------------------------------


def test_one_dream_per_repo_per_day(conn):
    triage_run(conn, "sentry:DRM-5")

    first = dream.enqueue(conn, repo="feliu-dev", days=7)
    second = dream.enqueue(conn, repo="feliu-dev", days=7)

    assert first is not None and first.startswith("dream:feliu-dev:")
    assert second is None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n FROM agent_runs WHERE kind = %s",
            (jobs.DREAM_KIND,),
        )
        assert cur.fetchone()["n"] == 1


def test_no_recent_activity_means_no_dream(conn):
    assert dream.enqueue(conn, repo="feliu-dev", days=7) is None


def test_a_dry_run_enqueues_nothing(conn):
    triage_run(conn, "sentry:DRM-6")
    assert dream.enqueue(conn, repo="feliu-dev", days=7, dry_run=True)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n FROM agent_runs WHERE kind = %s",
            (jobs.DREAM_KIND,),
        )
        assert cur.fetchone()["n"] == 0


# --- the job spec --------------------------------------------------------------


def dream_run(conn):
    triage_run(
        conn,
        "sentry:DRM-7",
        result={"outcome": "NOT_A_BUG", "reason": "already fixed on main"},
    )
    subject = dream.enqueue(conn, repo="feliu-dev", days=7)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM agent_runs WHERE kind = %s AND subject = %s",
            (jobs.DREAM_KIND, subject),
        )
        return get_run(conn, cur.fetchone()["id"])


def test_the_dream_spec_reads_the_repo_without_docker(conn):
    run = dream_run(conn)
    spec = jobs.build_spec(run)
    assert spec.repo == "feliu-dev"
    assert spec.branch == f"agent/run-{run.id}"
    assert spec.needs_github is True
    assert spec.needs_docker is False
    assert spec.reuse_branch is False


def test_the_dream_prompt_states_the_classes_and_the_no_delete_rule(conn):
    prompt = jobs.build_spec(dream_run(conn)).prompt
    assert "CONTRADICTED" in prompt and "STALE" in prompt and "MISSING" in prompt
    assert "NEVER delete or" in prompt
    assert "Edit ONLY CLAUDE.md" in prompt
    assert "already fixed on main" in prompt, "the digest evidence is in the brief"
    assert '"outcome": "CLEAN" | "FINDINGS"' in prompt
    assert "# Durability markers (MANDATORY)" in prompt


# --- the chain ------------------------------------------------------------------


def test_findings_park_for_the_human(conn):
    run = dream_run(conn)
    decision = jobs.followups(
        run,
        {
            "outcome": "FINDINGS",
            "pr_url": "https://github.com/x/pull/9",
            "flagged": [{"class": "CONTRADICTED", "claim": "X", "evidence": "Y"}],
            "summary": "one stale citation fixed, one contradiction flagged",
        },
    )
    assert decision.enqueue == ()
    assert decision.human_gate["pr_url"] == "https://github.com/x/pull/9"
    assert decision.human_gate["flagged"][0]["class"] == "CONTRADICTED"


def test_a_pr_alone_still_parks(conn):
    decision = jobs.followups(
        dream_run(conn),
        {"outcome": "FINDINGS", "pr_url": "https://github.com/x/pull/9"},
    )
    assert decision.human_gate is not None


def test_clean_completes_quietly(conn):
    decision = jobs.followups(
        dream_run(conn), {"outcome": "CLEAN", "applied": [], "flagged": []}
    )
    assert decision.enqueue == () and decision.human_gate is None


# --- the emails -----------------------------------------------------------------


def _finish_dream(conn, run, result):
    append(
        conn,
        run.id,
        EventType.STAGE_COMPLETED,
        {"outcome": "SUCCEEDED", "exit_code": 0, "result": result},
    )
    append(conn, run.id, EventType.RUN_DONE)
    decision = jobs.followups(run, result)
    if decision.human_gate:
        append(conn, run.id, EventType.HUMAN_GATE, decision.human_gate)


def test_a_dream_with_findings_emails_the_flags(conn):
    run = dream_run(conn)
    notify.pass_once(conn, lambda e: None, to="j@x", cap=10)  # flush the fixture run
    _finish_dream(
        conn,
        run,
        {
            "outcome": "FINDINGS",
            "pr_url": "https://github.com/x/pull/9",
            "flagged": [
                {"class": "CONTRADICTED", "claim": "the pool is 5", "evidence": "code"}
            ],
            "summary": "memory drifted",
        },
    )
    sent = []
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 1
    assert "memory audit found issues to review" in sent[0].body
    assert "[CONTRADICTED] the pool is 5" in sent[0].body
    assert "https://github.com/x/pull/9" in sent[0].body


def test_a_clean_dream_still_says_so_once(conn):
    run = dream_run(conn)
    notify.pass_once(conn, lambda e: None, to="j@x", cap=10)  # flush the fixture run
    _finish_dream(conn, run, {"outcome": "CLEAN", "applied": [], "flagged": []})
    sent = []
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 1
    assert "memory audit: CLEAN" in sent[0].subject
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 0, "exactly once"


def test_a_parked_pr_revision_emails_too(conn):
    """The #10 gap this branch closes: pr_revision was not in the notifier's
    kinds, so a revision that parked AWAITING_HUMAN would never email."""
    run_id = create_run(
        conn,
        jobs.PR_REVISION_KIND,
        "pr:feliu-dev#900",
        {"repo": "feliu-dev", "pr_url": "https://github.com/x/pull/900"},
    )
    append(
        conn,
        run_id,
        EventType.HUMAN_GATE,
        {"why": "change requests could not or should not be addressed"},
    )
    assert get_run(conn, run_id).status is RunStatus.AWAITING_HUMAN
    sent = []
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 1
    assert "change requests could not" in sent[0].body
