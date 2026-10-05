"""The dreaming job (#15): digest, enqueue, spec, chain, and its emails.

The invariants that matter: the digest is distilled facts and never raw
transcript, one dream per repo per day ever, flags are never auto-applied,
and neither a finding nor a clean pass is ever silent.
"""

import json
from contextlib import nullcontext
from datetime import timedelta

from orchestrator import dream, jobs, notify, queue
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


# --- finding the still-open memory PR (#45) -------------------------------------


BOT = "wanderindev-managed-agents[bot]"


class FakePulls:
    def __init__(self, pulls=(), *, closed=(), files=None, default_branch="main"):
        self._pulls = list(pulls)
        self._closed = list(closed)  # pages of closed PRs
        self._files = files or {}
        self._default = default_branch
        self.asked = []
        self.pages = []

    def open_pulls(self, full_repo):
        self.asked.append(full_repo)
        return self._pulls

    def repository(self, full_repo):
        return {"full_name": full_repo, "default_branch": self._default}

    def closed_pulls(self, full_repo, page=1):
        self.pages.append(page)
        return self._closed[page - 1] if page <= len(self._closed) else []

    def files(self, full_repo, number):
        return self._files.get(number, [])


def pull(number, *, head, author=BOT):
    return {
        "number": number,
        "user": {"login": author},
        "head": {"ref": head},
        "html_url": f"https://github.com/wanderindev/feliu-dev/pull/{number}",
    }


def dreamed(conn, subject="dream:feliu-dev:2026-08-06"):
    """A past dream run, so its branch resolves to a memory PR."""
    return create_run(conn, jobs.DREAM_KIND, subject, {"repo": "feliu-dev"})


def test_the_open_memory_pr_is_found_by_its_branch(conn):
    run_id = dreamed(conn)
    client = FakePulls([pull(167, head=f"agent/run-{run_id}")])

    found = dream.open_memory_pr(conn, client, repo="feliu-dev", bot_login=BOT)

    assert found == {
        "number": 167,
        "branch": f"agent/run-{run_id}",
        "url": "https://github.com/wanderindev/feliu-dev/pull/167",
    }
    assert client.asked == ["wanderindev/feliu-dev"]


def test_only_this_orchestrators_memory_prs_count(conn):
    dream_id = dreamed(conn)
    fix_id = create_run(conn, sentry.RUN_KIND, "sentry:DRM-x", {"repo": "feliu-dev"})
    client = FakePulls(
        [
            pull(10, head="feature/hand-written"),  # not an agent branch
            pull(11, head=f"agent/run-{fix_id}"),  # a fix PR, not a memory PR
            pull(12, head=f"agent/run-{dream_id}", author="wanderindev"),  # a human's
            pull(13, head="agent/run-999999"),  # no such run
        ]
    )

    assert dream.open_memory_pr(conn, client, repo="feliu-dev", bot_login=BOT) is None


def test_the_oldest_memory_pr_wins(conn):
    first, second = dreamed(conn, "dream:feliu-dev:d1"), dreamed(conn, "dream:x:d2")
    client = FakePulls(
        [pull(9, head=f"agent/run-{second}"), pull(4, head=f"agent/run-{first}")]
    )

    found = dream.open_memory_pr(conn, client, repo="feliu-dev", bot_login=BOT)

    assert found["number"] == 4


def test_an_unreachable_github_does_not_stop_the_audit(conn):
    class Broken:
        def open_pulls(self, full_repo):
            raise OSError("connection reset")

    assert dream.open_memory_pr(conn, Broken(), repo="feliu-dev") is None


def test_the_open_pr_is_recorded_in_the_payload(conn):
    triage_run(conn, "sentry:DRM-8")
    run_id = dreamed(conn)
    client = FakePulls([pull(167, head=f"agent/run-{run_id}")])

    subject = dream.enqueue(conn, repo="feliu-dev", days=7, pulls=client)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM agent_runs WHERE kind = %s AND subject = %s",
            (jobs.DREAM_KIND, subject),
        )
        run = get_run(conn, cur.fetchone()["id"])
    assert run.payload["open_pr"]["number"] == 167


def test_no_open_pr_leaves_the_payload_alone(conn):
    triage_run(conn, "sentry:DRM-9")
    subject = dream.enqueue(conn, repo="feliu-dev", days=7, pulls=FakePulls())
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM agent_runs WHERE kind = %s AND subject = %s",
            (jobs.DREAM_KIND, subject),
        )
        run = get_run(conn, cur.fetchone()["id"])
    assert "open_pr" not in run.payload


# --- the job spec --------------------------------------------------------------


def dream_run(conn, *, open_pr=None):
    triage_run(
        conn,
        "sentry:DRM-7",
        result={"outcome": "NOT_A_BUG", "reason": "already fixed on main"},
    )
    pulls = FakePulls([open_pr]) if open_pr else None
    subject = dream.enqueue(conn, repo="feliu-dev", days=7, pulls=pulls)
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
    assert '"outcome": "CLEAN" | "FINDINGS" | "NO_CHANGE"' in prompt
    assert "# Durability markers (MANDATORY)" in prompt


def test_without_an_open_pr_the_dream_opens_one(conn):
    prompt = jobs.build_spec(dream_run(conn)).prompt
    assert "gh pr create --draft" in prompt
    assert "An earlier audit's pull request is still open" not in prompt
    assert "gh pr edit" not in prompt and "NO_CHANGE" not in prompt.split("# Result")[0]


def test_an_open_pr_makes_the_dream_accumulate_onto_its_branch(conn):
    earlier = dreamed(conn)
    run = dream_run(conn, open_pr=pull(167, head=f"agent/run-{earlier}"))
    spec = jobs.build_spec(run)

    # The branch is the PR's, checked out as-is: the audit has to see the
    # edits already applied or it derives them a second time.
    assert spec.branch == f"agent/run-{earlier}"
    assert spec.reuse_branch is True
    assert "PR #167" in spec.prompt
    assert "git merge --no-edit origin/main" in spec.prompt
    assert "gh pr edit 167" in spec.prompt
    assert "gh pr comment 167" in spec.prompt
    assert "do NOT open a second pull" in spec.prompt
    assert "gh pr create" not in spec.prompt


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


def test_no_change_does_not_re_park_the_same_findings(conn):
    # The run that only re-verified an open PR must not park: the same flags
    # would otherwise be emailed every week the PR sits unmerged.
    decision = jobs.followups(
        dream_run(conn),
        {
            "outcome": "NO_CHANGE",
            "pr_url": "https://github.com/x/pull/167",
            "flagged": [{"class": "DELETION", "claim": "/search row"}],
            "summary": "re-verified; nothing new",
        },
    )
    assert decision.enqueue == () and decision.human_gate is None


# --- the CLAUDE.md size guard (#61) ----------------------------------------------


def test_the_prompt_carries_the_size_guard(conn, monkeypatch):
    monkeypatch.setattr(jobs.config, "DREAM_CLAUDE_MD_MAX_CHARS", 12345)
    prompt = jobs.build_spec(dream_run(conn)).prompt
    assert "at or under 12345 characters" in prompt
    assert "even with nothing else to change" in prompt
    assert "docs/claude/<topic>.md" in prompt
    assert "plus `docs/claude/*.md` for the size guard alone" in prompt
    assert '"SIZE"' in prompt


def test_the_size_limit_defaults_to_claude_codes_warning_floor():
    assert jobs.config.DREAM_CLAUDE_MD_MAX_CHARS == 40_000


def test_an_oversized_claude_md_parks_even_a_clean_run(conn, monkeypatch):
    monkeypatch.setattr(jobs.config, "DREAM_CLAUDE_MD_MAX_CHARS", 40_000)
    decision = jobs.followups(
        dream_run(conn),
        {"outcome": "CLEAN", "applied": [], "flagged": [], "claude_md_chars": 52_000},
    )
    gate = decision.human_gate
    assert gate is not None
    assert gate["why"].startswith("CLAUDE.md still 52,000 chars, over the 40,000")
    assert "issues to review" not in gate["why"]
    assert gate["claude_md_chars"] == 52_000
    assert gate["claude_md_max_chars"] == 40_000


def test_an_oversized_claude_md_parks_even_a_no_change_run(conn):
    decision = jobs.followups(
        dream_run(conn),
        {
            "outcome": "NO_CHANGE",
            "pr_url": "https://github.com/x/pull/167",
            "claude_md_chars": 40_001,
        },
    )
    assert decision.human_gate is not None
    assert "issues to review" not in decision.human_gate["why"]


def test_findings_and_an_oversized_file_say_both(conn):
    decision = jobs.followups(
        dream_run(conn),
        {
            "outcome": "FINDINGS",
            "pr_url": "https://github.com/x/pull/9",
            "claude_md_chars": 90_000,
        },
    )
    why = decision.human_gate["why"]
    assert "CLAUDE.md still 90,000 chars" in why
    assert "memory audit found issues to review" in why


def test_a_claude_md_at_the_limit_is_not_flagged(conn):
    decision = jobs.followups(
        dream_run(conn),
        {"outcome": "CLEAN", "applied": [], "flagged": [], "claude_md_chars": 40_000},
    )
    assert decision.human_gate is None


def test_a_dream_that_split_claude_md_parks_without_the_size_flag(conn):
    decision = jobs.followups(
        dream_run(conn),
        {
            "outcome": "FINDINGS",
            "pr_url": "https://github.com/x/pull/9",
            "applied": [{"class": "SIZE", "claim": "Blog section", "edit": "moved"}],
            "claude_md_chars": 31_000,
        },
    )
    gate = decision.human_gate
    assert gate["why"] == "memory audit found issues to review"
    assert "claude_md_chars" not in gate


def test_an_oversized_dream_emails_the_size(conn):
    run = dream_run(conn)
    notify.pass_once(conn, lambda e: None, to="j@x", cap=10)  # flush the fixture run
    _finish_dream(
        conn,
        run,
        {"outcome": "CLEAN", "applied": [], "flagged": [], "claude_md_chars": 52_000},
    )
    sent = []
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 1
    assert "CLAUDE.md still 52,000 chars" in sent[0].subject


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


def test_a_no_change_dream_says_so_once_and_quietly(conn):
    run = dream_run(conn)
    notify.pass_once(conn, lambda e: None, to="j@x", cap=10)  # flush the fixture run
    _finish_dream(
        conn,
        run,
        {
            "outcome": "NO_CHANGE",
            "pr_url": "https://github.com/x/pull/167",
            "flagged": [{"class": "DELETION", "claim": "/search row"}],
            "summary": "re-verified the open PR; nothing new",
        },
    )
    sent = []
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 1
    assert "nothing new" in sent[0].subject
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


# --- weekly, multi-repo, merged PRs as evidence (#60) ----------------------------


def merged(number, merged_at, *, title="a change", author="wanderindev"):
    return {
        "number": number,
        "title": title,
        "user": {"login": author},
        "merged_at": merged_at,
        "updated_at": merged_at,
    }


def iso(when):
    return when.isoformat().replace("+00:00", "Z")


def run_by_subject(conn, subject):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM agent_runs WHERE subject = %s", (subject,))
        return get_run(conn, cur.fetchone()["id"])


def test_merged_pulls_keeps_only_merges_inside_the_window(conn):
    now = queue.db_now(conn)
    inside = iso(now - timedelta(days=2))
    outside = iso(now - timedelta(days=9))
    closed_unmerged = {"number": 3, "merged_at": None, "updated_at": inside}
    client = FakePulls(
        closed=[
            [merged(5, inside, title="Add X"), closed_unmerged, merged(2, outside)]
        ],
        files={
            5: [
                {"filename": "a.py", "additions": 3, "deletions": 1},
                {"filename": "b.md", "additions": 2, "deletions": 0},
            ]
        },
    )

    found, total = dream.merged_pulls(
        client, repo="feliu-dev", since=now - timedelta(days=7)
    )

    assert found == [
        {
            "number": 5,
            "title": "Add X",
            "author": "wanderindev",
            "merged_at": inside,
            "changed": {
                "count": 2,
                "additions": 5,
                "deletions": 1,
                "files": ["a.py", "b.md"],
            },
        }
    ]
    assert total == 1
    assert client.pages == [1], "a short page is the last one"


def test_merged_pulls_pages_until_the_window_is_passed(conn):
    now = queue.db_now(conn)
    recent = iso(now - timedelta(days=1))
    old = iso(now - timedelta(days=30))
    full_page = [merged(1000 + n, recent) for n in range(100)]
    client = FakePulls(closed=[full_page, [merged(7, old)] * 100, [merged(8, recent)]])

    found, total = dream.merged_pulls(
        client, repo="feliu-dev", since=now - timedelta(days=7)
    )

    assert client.pages == [1, 2], "page 2 reached back past the window"
    assert len(found) == dream.MAX_MERGED_PRS
    assert total == 100
    assert found[0]["changed"] == {
        "count": 0,
        "additions": 0,
        "deletions": 0,
        "files": [],
    }


def test_a_big_pr_lists_some_files_and_counts_the_rest(conn):
    now = queue.db_now(conn)
    files = [{"filename": f"f{n}.py"} for n in range(20)]
    client = FakePulls(closed=[[merged(1, iso(now))]], files={1: files})

    (entry,), _ = dream.merged_pulls(client, repo="x", since=now - timedelta(days=1))

    assert entry["changed"]["count"] == 20
    assert len(entry["changed"]["files"]) == 15
    assert entry["changed"]["more_files"] == 5


def test_github_failures_never_stop_the_merged_pr_digest(conn):
    now = queue.db_now(conn)

    class Flaky(FakePulls):
        def files(self, full_repo, number):
            raise OSError("reset")

    flaky = Flaky(closed=[[merged(1, iso(now))]])
    (entry,), _ = dream.merged_pulls(flaky, repo="x", since=now - timedelta(days=1))
    assert entry["changed"] is None

    class Down(FakePulls):
        def closed_pulls(self, full_repo, page=1):
            raise OSError("down")

    assert dream.merged_pulls(Down(), repo="x", since=now) == ([], 0)


def test_the_default_branch_comes_from_github(conn):
    client = FakePulls(default_branch="master")
    assert dream.default_branch(client, repo="x") == "master"

    class Down(FakePulls):
        def repository(self, full_repo):
            raise OSError("down")

    assert dream.default_branch(Down(), repo="x") == "main"


def test_a_repo_with_merges_but_no_runs_still_dreams(conn):
    client = FakePulls(
        closed=[[merged(12, iso(queue.db_now(conn)), title="Interactive work")]],
        default_branch="master",
    )

    subject = dream.enqueue(conn, repo="atelier-new-cli", days=7, pulls=client)

    assert subject and subject.startswith("dream:atelier-new-cli:")
    run = run_by_subject(conn, subject)
    assert run.payload["repo"] == "atelier-new-cli", "coalescing keys on repo (#59)"
    assert run.payload["digest"] == []
    assert run.payload["merged_prs"][0]["title"] == "Interactive work"
    assert run.payload["base_branch"] == "master"


def test_fit_evidence_trims_oldest_runs_before_prs():
    runs = [{"run": n, "pad": "r" * 90} for n in range(5)]  # newest first
    prs = [{"run": n, "pad": "p" * 90} for n in range(3)]
    line = len(json.dumps(runs[0]).encode()) + 1

    kept_runs, kept_prs = dream.fit_evidence(runs, prs, max_bytes=5 * line)

    assert [r["run"] for r in kept_runs] == [0, 1], "the oldest runs go first"
    assert kept_prs == prs

    kept_runs, kept_prs = dream.fit_evidence(runs, prs, max_bytes=2 * line)
    assert kept_runs == [] and [p["run"] for p in kept_prs] == [0, 1]


def test_an_oversized_week_is_trimmed_under_the_argv_bound(conn):
    """#60 review: ~5 KB per result-bearing run times 40, plus 40 merged PRs,
    blew past MAX_ARG_STRLEN (128 KiB) for `claude -p "$PROMPT"`."""
    big = {"summary": "s" * 590, "reason": "r" * 590, "fix": "f" * 590}
    for n in range(dream.MAX_DIGEST_RUNS):
        triage_run(
            conn,
            f"sentry:DRM-big-{n}",
            result={**big, "a": "a" * 590, "b": "b" * 590, "c": "c" * 590},
            stages=tuple(
                json.dumps({"stage": f"s{k}", "note": "n" * 590}) for k in range(3)
            ),
            gate={"why": "w" * 590, "detail": "d" * 590},
        )
    now = queue.db_now(conn)
    files = [{"filename": f"src/{'x' * 80}/{n}.py"} for n in range(15)]
    client = FakePulls(
        closed=[[merged(n, iso(now), title="t" * 590) for n in range(50)]],
        files={n: files for n in range(50)},
    )

    run = run_by_subject(
        conn, dream.enqueue(conn, repo="feliu-dev", days=7, pulls=client)
    )

    evidence = run.payload["digest"] + run.payload["merged_prs"]
    size = sum(len(json.dumps(e).encode()) + 1 for e in evidence)
    assert size <= dream.MAX_EVIDENCE_BYTES
    assert run.payload["runs_omitted"] == dream.MAX_DIGEST_RUNS - len(
        run.payload["digest"]
    )
    assert run.payload["runs_omitted"] > 0
    assert run.payload["prs_omitted"] == 50 - len(run.payload["merged_prs"])
    prompt = jobs.build_spec(run).prompt
    assert len(prompt.encode()) < 128 * 1024, "fits one argv string"
    assert "older merged PR(s) in the window are not shown" in prompt


def test_a_busy_week_says_how_many_runs_it_left_out(conn):
    for n in range(dream.MAX_DIGEST_RUNS + 2):
        triage_run(conn, f"sentry:DRM-busy-{n}")

    run = run_by_subject(conn, dream.enqueue(conn, repo="feliu-dev", days=7))

    assert len(run.payload["digest"]) == dream.MAX_DIGEST_RUNS
    assert run.payload["runs_omitted"] == 2
    assert "2 older run(s) in the window are not shown" in jobs.build_spec(run).prompt


def test_without_github_the_base_is_main_and_there_are_no_merges(conn):
    run = dream_run(conn)
    assert run.payload["base_branch"] == "main"
    assert run.payload["merged_prs"] == []
    assert "runs_omitted" not in run.payload
    spec = jobs.build_spec(run)
    assert spec.base_branch == "main"
    assert "(no merged pull requests)" in spec.prompt


def test_the_dream_targets_the_repos_default_branch(conn):
    client = FakePulls(
        closed=[[merged(12, iso(queue.db_now(conn)), title="Rename the CLI flag")]],
        default_branch="master",
    )
    subject = dream.enqueue(conn, repo="atelier-new-cli", days=7, pulls=client)

    spec = jobs.build_spec(run_by_subject(conn, subject))

    assert spec.base_branch == "master"
    assert "--base master" in spec.prompt
    assert "Never push to master" in spec.prompt
    assert "--base main" not in spec.prompt
    assert "Rename the CLI flag" in spec.prompt, "merged PRs are in the brief"


def test_an_open_pr_on_a_master_repo_merges_origin_master(conn):
    earlier = create_run(
        conn, jobs.DREAM_KIND, "dream:atelier-new-cli:d0", {"repo": "atelier-new-cli"}
    )
    client = FakePulls(
        [pull(3, head=f"agent/run-{earlier}")],
        closed=[[merged(12, iso(queue.db_now(conn)))]],
        default_branch="master",
    )
    subject = dream.enqueue(conn, repo="atelier-new-cli", days=7, pulls=client)

    prompt = jobs.build_spec(run_by_subject(conn, subject)).prompt

    assert "merge --no-edit origin/master" in prompt
    assert "origin/main" not in prompt


def test_a_pre_60_payload_still_builds(conn):
    run_id = create_run(
        conn,
        jobs.DREAM_KIND,
        "dream:feliu-dev:old",
        {"repo": "feliu-dev", "days": 1, "digest": []},
    )
    spec = jobs.build_spec(get_run(conn, run_id))
    assert spec.base_branch == "main"
    assert "--base main" in spec.prompt


def test_the_dream_runs_on_opus_5_5(conn):
    assert jobs.build_spec(dream_run(conn)).model == "claude-opus-5-5"


def test_the_dream_list_replaces_recent_repos(conn, monkeypatch, tmp_path):
    triage_run(conn, "sentry:DRM-list", repo="feliu-dev")
    monkeypatch.setattr(dream.config, "DREAM_REPOS", ("atelier-theme", "pic-ext"))
    monkeypatch.setattr(dream.config, "CLAUDE_CREDENTIALS", str(tmp_path / "none"))
    monkeypatch.setattr(dream, "connect", lambda: nullcontext(conn))
    monkeypatch.setattr(dream, "_pulls_client", lambda: None)
    seen = []
    monkeypatch.setattr(dream, "enqueue", lambda conn, **kw: seen.append(kw["repo"]))

    assert dream.main([]) == 0
    assert seen == ["atelier-theme", "pic-ext"]

    seen.clear()
    monkeypatch.setattr(dream.config, "DREAM_REPOS", ())
    assert dream.main([]) == 0
    assert seen == ["feliu-dev"], "unset, it falls back to recent_repos"
