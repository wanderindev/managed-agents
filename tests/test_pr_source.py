"""The change-request loop (#10): poll, guards, spec, and chain wiring.

The boundaries are the tests that matter most here: never a human's PR, never
a branch a human pushed to, never more than three revision rounds, and never
the same comment handled twice.
"""

from orchestrator import jobs
from orchestrator.enums import EventType
from orchestrator.log import append, create_run
from orchestrator.queue import get_run
from orchestrator.sources import github_prs, sentry

BOT = "wanderindev-managed-agents[bot]"
HUMAN = "wanderindev"
REPO = "wanderindev/panama-in-context"

ISSUE_FACTS = {
    "issue_id": "5002",
    "short_id": "PIC-PYTHON-FASTAPI-1T",
    "title": "TruncationError: research_summary",
    "culprit": "app.services.research in summarize",
    "project": "pic-python-fastapi",
    "repo": "panama-in-context",
    "permalink": "https://javier-feliu.sentry.io/issues/5002/",
}


def pr(number, *, author=BOT, head, url=None):
    return {
        "number": number,
        "user": {"login": author},
        "head": {"ref": head},
        "html_url": url or f"https://github.com/{REPO}/pull/{number}",
    }


def review(*, state="CHANGES_REQUESTED", author=HUMAN, at, body="tighten this"):
    return {
        "state": state,
        "user": {"login": author},
        "submitted_at": at,
        "body": body,
        "html_url": "https://github.com/r",
    }


def comment(*, author=HUMAN, at, body="rename this", path="backend/app/x.py", line=12):
    return {
        "user": {"login": author},
        "created_at": at,
        "body": body,
        "path": path,
        "line": line,
        "html_url": "https://github.com/c",
    }


def commit(author_login, committer_login=None):
    return {
        "author": {"login": author_login},
        "committer": {"login": committer_login or author_login},
    }


class FakePulls:
    def __init__(self, pulls=(), reviews=(), comments=(), commits=()):
        self._pulls = list(pulls)
        self._reviews = list(reviews)
        self._comments = list(comments)
        self._commits = list(commits) or [commit("claude")]

    def open_pulls(self, full_repo):
        return self._pulls

    def reviews(self, full_repo, number):
        return self._reviews

    def review_comments(self, full_repo, number):
        return self._comments

    def commits(self, full_repo, number):
        return self._commits


def origin_run(conn):
    """The triage run whose branch the PR under test is."""
    run_id = create_run(conn, sentry.RUN_KIND, "sentry:PRS-origin", ISSUE_FACTS)
    append(conn, run_id, EventType.RUN_DONE)
    return run_id


def poll(conn, client, **kwargs):
    kwargs.setdefault("repos", (REPO,))
    kwargs.setdefault("bot_login", BOT)
    kwargs.setdefault("human_logins", (HUMAN,))
    kwargs.setdefault("max_per_poll", 3)
    return github_prs.poll(conn, client, **kwargs)


def latest_revision_run(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM agent_runs WHERE kind = %s ORDER BY id DESC LIMIT 1",
            (github_prs.RUN_KIND,),
        )
        return get_run(conn, cur.fetchone()["id"])


# --- enqueueing ---------------------------------------------------------------


def test_a_changes_requested_review_enqueues_a_revision(conn):
    origin = origin_run(conn)
    client = FakePulls(
        pulls=[pr(600, head=f"agent/run-{origin}")],
        reviews=[review(at="2026-07-27T10:00:00Z")],
        comments=[comment(at="2026-07-27T10:00:30Z")],
    )

    report = poll(conn, client)

    assert report.enqueued == ["pr:panama-in-context#600"]
    run = latest_revision_run(conn)
    payload = run.payload
    assert payload["short_id"] == ISSUE_FACTS["short_id"]
    assert payload["issue_id"] == ISSUE_FACTS["issue_id"]
    assert payload["repo"] == "panama-in-context"
    assert payload["branch"] == f"agent/run-{origin}"
    assert payload["number"] == 600
    assert [c["kind"] for c in payload["change_requests"]] == ["review", "comment"]
    assert payload["cursor"] == "2026-07-27T10:00:30Z"
    assert payload["revision"] == 1
    assert payload["round"] == 1


def test_a_dry_run_enqueues_nothing(conn):
    origin = origin_run(conn)
    client = FakePulls(
        pulls=[pr(601, head=f"agent/run-{origin}")],
        reviews=[review(at="2026-07-27T10:00:00Z")],
    )

    report = poll(conn, client, dry_run=True)

    assert report.enqueued == ["pr:panama-in-context#601"]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n FROM agent_runs WHERE kind = %s",
            (github_prs.RUN_KIND,),
        )
        assert cur.fetchone()["n"] == 0


# --- the guards ---------------------------------------------------------------


def test_a_humans_pr_is_never_touched(conn):
    client = FakePulls(
        pulls=[pr(602, author=HUMAN, head="fix/by-javier")],
        reviews=[review(at="2026-07-27T10:00:00Z")],
    )

    report = poll(conn, client)

    assert report.enqueued == []
    assert report.dropped == {"not authored by this orchestrator": 1}


def test_a_branch_with_human_commits_is_left_alone(conn):
    origin = origin_run(conn)
    client = FakePulls(
        pulls=[pr(603, head=f"agent/run-{origin}")],
        reviews=[review(at="2026-07-27T10:00:00Z")],
        commits=[commit("claude"), commit(HUMAN)],
    )

    report = poll(conn, client)

    assert report.enqueued == []
    assert report.dropped == {"human commits on the branch; not touching it": 1}


def test_approvals_and_plain_comment_reviews_do_not_trigger(conn):
    origin = origin_run(conn)
    client = FakePulls(
        pulls=[pr(604, head=f"agent/run-{origin}")],
        reviews=[
            review(state="APPROVED", at="2026-07-27T10:00:00Z"),
            review(state="COMMENTED", at="2026-07-27T10:01:00Z"),
        ],
    )

    report = poll(conn, client)

    assert report.enqueued == []
    assert report.dropped == {"no new change requests": 1}


def test_the_bots_own_activity_is_not_a_change_request(conn):
    """The revision agent replies on the PR; that reply must not re-trigger."""
    origin = origin_run(conn)
    client = FakePulls(
        pulls=[pr(605, head=f"agent/run-{origin}")],
        comments=[comment(author=BOT, at="2026-07-27T10:00:00Z")],
    )

    assert poll(conn, client).enqueued == []


def test_a_foreign_head_branch_is_dropped(conn):
    client = FakePulls(
        pulls=[pr(606, head="feat/hand-made")],
        reviews=[review(at="2026-07-27T10:00:00Z")],
    )

    report = poll(conn, client)

    assert report.enqueued == []
    assert report.dropped == {"head branch is not an agent branch": 1}


# --- the cursor and the bound --------------------------------------------------


def _finish(conn, run_id):
    append(conn, run_id, EventType.RUN_DONE)


def test_handled_comments_do_not_retrigger(conn):
    origin = origin_run(conn)
    client = FakePulls(
        pulls=[pr(607, head=f"agent/run-{origin}")],
        reviews=[review(at="2026-07-27T10:00:00Z")],
    )
    assert poll(conn, client).enqueued == ["pr:panama-in-context#607"]
    _finish(conn, latest_revision_run(conn).id)

    report = poll(conn, client)

    assert report.enqueued == []
    assert report.dropped == {"no new change requests": 1}


def test_a_newer_comment_reopens_the_loop_with_only_the_fresh_items(conn):
    origin = origin_run(conn)
    first = FakePulls(
        pulls=[pr(608, head=f"agent/run-{origin}")],
        reviews=[review(at="2026-07-27T10:00:00Z")],
    )
    poll(conn, first)
    _finish(conn, latest_revision_run(conn).id)

    second = FakePulls(
        pulls=[pr(608, head=f"agent/run-{origin}")],
        reviews=[review(at="2026-07-27T10:00:00Z")],
        comments=[comment(at="2026-07-27T12:00:00Z", body="also rename Y")],
    )
    report = poll(conn, second)

    assert report.enqueued == ["pr:panama-in-context#608"]
    payload = latest_revision_run(conn).payload
    assert [c["body"] for c in payload["change_requests"]] == ["also rename Y"]
    assert payload["revision"] == 2


def test_an_in_flight_revision_blocks_a_second(conn):
    origin = origin_run(conn)
    first = FakePulls(
        pulls=[pr(609, head=f"agent/run-{origin}")],
        reviews=[review(at="2026-07-27T10:00:00Z")],
    )
    poll(conn, first)  # queued, not finished

    second = FakePulls(
        pulls=[pr(609, head=f"agent/run-{origin}")],
        reviews=[review(at="2026-07-27T10:00:00Z")],
        comments=[comment(at="2026-07-27T12:00:00Z")],
    )
    report = poll(conn, second)

    assert report.enqueued == []
    assert report.dropped == {"a revision is already in flight": 1}


def test_three_rounds_is_the_ceiling(conn):
    origin = origin_run(conn)
    for hour in (10, 12, 14):
        client = FakePulls(
            pulls=[pr(610, head=f"agent/run-{origin}")],
            reviews=[review(at=f"2026-07-27T{hour}:00:00Z")],
        )
        assert poll(conn, client).enqueued, f"round at {hour}h should enqueue"
        _finish(conn, latest_revision_run(conn).id)

    fourth = FakePulls(
        pulls=[pr(610, head=f"agent/run-{origin}")],
        reviews=[review(at="2026-07-27T16:00:00Z")],
    )
    report = poll(conn, fourth)

    assert report.enqueued == []
    assert report.dropped == {"revision rounds exhausted; needs a human": 1}


# --- the job spec ---------------------------------------------------------------


def revision_run(conn):
    origin = origin_run(conn)
    client = FakePulls(
        pulls=[pr(611, head=f"agent/run-{origin}")],
        reviews=[review(at="2026-07-27T10:00:00Z", body="tighten the query")],
        comments=[comment(at="2026-07-27T10:00:30Z", body="rename fetch_x")],
    )
    poll(conn, client)
    return latest_revision_run(conn)


def test_the_revision_spec_reuses_the_branch_with_full_reach(conn):
    spec = jobs.build_spec(revision_run(conn))
    assert spec.repo == "panama-in-context"
    assert spec.reuse_branch is True
    assert spec.needs_github is True
    assert spec.needs_docker is True


def test_the_revision_prompt_briefs_the_change_requests(conn):
    run = revision_run(conn)
    prompt = jobs.build_spec(run).prompt
    assert "tighten the query" in prompt
    assert "rename fetch_x" in prompt
    assert "backend/app/x.py:12" in prompt
    assert "revision round 1 of 3" in prompt
    assert "what you deliberately did NOT change" in prompt
    assert "Never change the pull request's draft/ready state" in prompt
    assert "# Durability markers (MANDATORY)" in prompt
    assert '"outcome": "FIX" | "NEEDS_HUMAN"' in prompt


# --- the chain -------------------------------------------------------------------


def test_a_pushed_revision_gets_the_adversarial_treatment(conn):
    run = revision_run(conn)
    decision = jobs.followups(run, {"outcome": "FIX", "pr_url": run.payload["pr_url"]})
    assert len(decision.enqueue) == 1
    chained = decision.enqueue[0]
    assert chained.kind == jobs.REVIEW_KIND
    assert chained.subject == run.subject
    assert chained.payload["pr_url"] == run.payload["pr_url"]
    assert chained.payload["branch"] == run.payload["branch"]
    assert chained.payload["round"] == 1
    assert chained.payload["issue_id"] == ISSUE_FACTS["issue_id"]
    assert decision.human_gate is None


def test_a_declined_revision_parks_for_the_human(conn):
    run = revision_run(conn)
    decision = jobs.followups(
        run, {"outcome": "NEEDS_HUMAN", "reason": "the request would break auth"}
    )
    assert decision.enqueue == ()
    assert decision.human_gate["why"] == (
        "change requests could not or should not be addressed"
    )
    assert decision.human_gate["reason"] == "the request would break auth"


def test_an_unreachable_repo_loses_nothing_else(conn):
    origin = origin_run(conn)

    class Flaky(FakePulls):
        def open_pulls(self, full_repo):
            if full_repo == "wanderindev/feliu-dev":
                raise OSError("boom")
            return super().open_pulls(full_repo)

    client = Flaky(
        pulls=[pr(612, head=f"agent/run-{origin}")],
        reviews=[review(at="2026-07-27T10:00:00Z")],
    )
    report = poll(conn, client, repos=("wanderindev/feliu-dev", REPO))

    assert report.enqueued == ["pr:panama-in-context#612"]
    assert report.dropped == {"repo wanderindev/feliu-dev unreachable": 1}
